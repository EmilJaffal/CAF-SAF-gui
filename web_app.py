import builtins
import contextlib
import io
import importlib.util
import importlib
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
import zipfile
from pathlib import Path

# Prevent GUI backend usage (macOS NSWindow) in web-server threads.
os.environ.setdefault("MPLBACKEND", "Agg")

try:
    import matplotlib

    matplotlib.use("Agg", force=True)
except Exception:
    # Matplotlib may not be installed in minimal environments.
    pass

import click
import pandas as pd
from flask import Flask, after_this_request, render_template, request, send_file

from app.runners import feature, filter, match, merge, sort


BASE_DIR = Path(__file__).resolve().parent
JOBS_DIR = BASE_DIR / "web_jobs"
PERFORMANCE_APP_ROOT = BASE_DIR  # Local core/ directory for option 8

app = Flask(__name__)


class AnswerQueue:
    def __init__(self, answers_text: str):
        self.answers = [line.strip() for line in answers_text.splitlines()]
        self.index = 0

    def next_answer(self, default=None):
        while self.index < len(self.answers):
            value = self.answers[self.index]
            self.index += 1
            if value != "":
                return value
        if default is not None:
            return default
        raise RuntimeError(
            "Insufficient answers for interactive prompts. "
            "Add more lines in 'Prompt Answers'."
        )


def _convert_prompt_value(value, expected_type):
    if expected_type is None:
        return value
    if expected_type is int:
        return int(value)
    if expected_type is float:
        return float(value)
    if expected_type is bool:
        return str(value).strip().lower() in {"1", "true", "t", "y", "yes"}
    return value


@contextlib.contextmanager
def patched_prompts(answer_queue: AnswerQueue):
    original_input = builtins.input
    original_click_prompt = click.prompt
    original_click_confirm = click.confirm

    def fake_input(prompt=""):
        _ = prompt
        return answer_queue.next_answer()

    def fake_prompt(text, default=None, type=None, **kwargs):
        _ = kwargs
        raw = answer_queue.next_answer(default=default)
        return _convert_prompt_value(raw, type)

    def fake_confirm(text, default=False, **kwargs):
        _ = text, kwargs
        raw = str(answer_queue.next_answer(default="" if default is None else default))
        if raw == "":
            return bool(default)
        return str(raw).strip().lower() in {"1", "true", "t", "y", "yes"}

    builtins.input = fake_input
    click.prompt = fake_prompt
    click.confirm = fake_confirm
    try:
        yield
    finally:
        builtins.input = original_input
        click.prompt = original_click_prompt
        click.confirm = original_click_confirm


def _snapshot_files(root: Path):
    files = {}
    for path in root.rglob("*"):
        if path.is_file():
            rel = path.relative_to(root)
            stat = path.stat()
            files[str(rel)] = (stat.st_mtime_ns, stat.st_size)
    return files


def _zip_outputs(job_dir: Path, before_snapshot: dict, run_log_text: str):
    after_snapshot = _snapshot_files(job_dir)
    changed = []
    for rel_path, meta in after_snapshot.items():
        if rel_path not in before_snapshot or before_snapshot[rel_path] != meta:
            changed.append(rel_path)

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel in sorted(changed):
            zf.write(job_dir / rel, rel)
        zf.writestr("run.log", run_log_text)
    zip_buffer.seek(0)
    return zip_buffer, changed


def _changed_files_since_snapshot(job_dir: Path, before_snapshot: dict):
    after_snapshot = _snapshot_files(job_dir)
    changed = []
    for rel_path, meta in after_snapshot.items():
        if rel_path not in before_snapshot or before_snapshot[rel_path] != meta:
            changed.append(rel_path)
    return sorted(changed)


def _derive_option1_input_name(changed_files: list[str]):
    suffixes_to_strip = [
        "_elements_sorted",
        "_summary",
        "_filtered",
        "_errors",
        "_element_count",
        "_ptable",
    ]

    for rel in changed_files:
        path = Path(rel)
        if path.suffix.lower() not in {".xlsx", ".csv", ".png"}:
            continue
        name = path.stem
        for suffix in suffixes_to_strip:
            if name.endswith(suffix):
                name = name[: -len(suffix)]
        if name:
            return name

    for rel in changed_files:
        path = Path(rel)
        if len(path.parts) > 1 and path.parts[0]:
            return path.parts[0]

    return "input"


def _sanitize_name_component(name: str):
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in name)
    cleaned = cleaned.strip("_")
    return cleaned or "input"


def _derive_caf_input_name(changed_files: list[str]):
    suffixes_to_strip = [
        "_elements_sorted",
        "_summary",
        "_filtered",
        "_errors",
        "_element_count",
        "_ptable",
        "_merged",
        "_missing_files",
        "_descend",
        "_norm",
    ]

    for rel in changed_files:
        path = Path(rel)
        if path.suffix.lower() not in {".xlsx", ".xls", ".csv", ".png"}:
            continue
        name = path.stem

        if "_by_property_" in name:
            name = name.split("_by_property_", 1)[0]
        elif "_by_" in name:
            name = name.split("_by_", 1)[0]

        for suffix in suffixes_to_strip:
            if name.endswith(suffix):
                name = name[: -len(suffix)]

        name = _sanitize_name_component(name)
        if name:
            return name

    return "input"


def _derive_option2_generated_filename(changed_files: list[str]):
    for rel in changed_files:
        name = Path(rel).name
        if name.lower().endswith((".xlsx", ".xls", ".csv")) and "_by_" in name:
            return name
    for rel in changed_files:
        name = Path(rel).name
        if name.lower().endswith((".xlsx", ".xls", ".csv")):
            return name
    return "(no sorted file detected)"


def _list_xlsx_files_with_formula(script_dir_path: Path):
    excel_files_with_paths = []
    excel_files = [
        file
        for file in os.listdir(script_dir_path)
        if file.endswith(".xlsx")
    ]

    for file in excel_files:
        file_path = script_dir_path / file
        try:
            df = pd.read_excel(file_path, nrows=0)
            if any(str(col).lower() == "formula" for col in df.columns):
                excel_files_with_paths.append(file)
        except Exception:
            continue

    excel_files_with_paths.sort(key=lambda x: x.lower())
    return excel_files_with_paths


def _choose_preferred_xlsx_file(file_names: list[str], preferred_suffixes: list[str]):
    if not file_names:
        return None

    lowered = [f.lower() for f in file_names]
    for suffix in preferred_suffixes:
        suffix_lower = suffix.lower()
        for idx, name in enumerate(lowered):
            if name.endswith(suffix_lower):
                return file_names[idx]

    return file_names[0]


def _build_auto_option2_answers(job_dir: Path):
    xlsx_files = _list_xlsx_files_with_formula(job_dir)
    if not xlsx_files:
        raise RuntimeError(
            "Option 7 could not find an Excel file with a Formula column after option 1."
        )

    preferred = _choose_preferred_xlsx_file(
        xlsx_files,
        preferred_suffixes=["_filtered.xlsx", "_elements_sorted.xlsx"],
    )
    file_index = xlsx_files.index(preferred) + 1

    # Method 1 (custom label) and selected file index.
    return f"1\n{file_index}", preferred


def _build_auto_option3_answers(job_dir: Path):
    xlsx_files = _list_xlsx_files_with_formula(job_dir)
    if not xlsx_files:
        raise RuntimeError(
            "Option 7 could not find an Excel file with a Formula column after option 2."
        )

    preferred = _choose_preferred_xlsx_file(
        xlsx_files,
        preferred_suffixes=["_by_custom_label.xlsx"],
    )
    file_index = xlsx_files.index(preferred) + 1

    # Select file, and keep extended features at default (No).
    return f"{file_index}\nn", preferred


def _pick_option3_caf_csv(job_dir: Path, changed_option3: list[str]):
    candidates = []
    for rel in changed_option3:
        rel_path = Path(rel)
        if rel_path.suffix.lower() != ".csv":
            continue
        if _is_ignored_rel_path(rel_path):
            continue
        candidates.append(rel_path)

    if not candidates:
        return None

    priorities = ["ternary", "universal", "binary", "quaternary"]
    for keyword in priorities:
        for rel_path in candidates:
            if keyword in rel_path.name.lower():
                return job_dir / rel_path

    return job_dir / candidates[0]


def _pick_option6_saf_csvs(job_dir: Path, changed_option6: list[str]):
    candidates = []
    for rel in changed_option6:
        rel_path = Path(rel)
        if rel_path.suffix.lower() != ".csv":
            continue
        if _is_ignored_rel_path(rel_path):
            continue
        if "csv" not in rel_path.parts:
            continue
        candidates.append(job_dir / rel_path)

    # Option 7 should merge ternary CAF output with ternary SAF output.
    ternary = [p for p in candidates if "ternary" in p.name.lower()]
    if ternary:
        return sorted(ternary, key=lambda p: str(p).lower())

    return sorted(candidates, key=lambda p: str(p).lower())


def _find_common_merge_column(left_df: pd.DataFrame, right_df: pd.DataFrame):
    left_cols = {str(col).lower(): str(col) for col in left_df.columns}
    right_cols = {str(col).lower(): str(col) for col in right_df.columns}

    preferred_keys = ["entry", "cif_id", "id"]

    for key in preferred_keys:
        if key in left_cols and key in right_cols:
            return left_cols[key], right_cols[key], key

    return None, None, None


def _normalize_merge_key_series(series: pd.Series):
    normalized = series.astype(str).str.strip()
    normalized = normalized.str.replace(r"\.0+$", "", regex=True)

    # SAF/CAF IDs can appear as raw numbers, floats, or filenames like 12345.cif.
    digits = normalized.str.extract(r"(\d+)", expand=False)
    normalized = digits.where(digits.notna() & (digits != ""), normalized.str.lower())

    return normalized


def _merge_option3_and_option6_like_option4(
    job_dir: Path,
    caf_csv_path: Path,
    saf_csv_paths: list[Path],
):
    if caf_csv_path is None or not caf_csv_path.exists():
        raise RuntimeError(
            "Option 7 could not find CAF option 3 CSV outputs to merge."
        )

    if not saf_csv_paths:
        raise RuntimeError(
            "Option 7 could not find SAF option 6 CSV outputs to merge."
        )

    caf_df = pd.read_csv(caf_csv_path)

    saf_frames = []
    for path in saf_csv_paths:
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if df.empty:
            continue
        df = df.copy()
        df["saf_source_file"] = path.name
        saf_frames.append(df)

    if not saf_frames:
        raise RuntimeError("Option 7 SAF outputs were empty or unreadable.")

    saf_df = pd.concat(saf_frames, ignore_index=True)

    caf_key, saf_key, merge_key_name = _find_common_merge_column(caf_df, saf_df)
    if not caf_key or not saf_key:
        raise RuntimeError(
            "Option 7 could not find a shared merge column between CAF and SAF outputs."
        )

    caf_tmp = caf_df.copy()
    saf_tmp = saf_df.copy()

    caf_tmp[caf_key] = _normalize_merge_key_series(caf_tmp[caf_key])
    saf_tmp[saf_key] = _normalize_merge_key_series(saf_tmp[saf_key])

    caf_tmp = caf_tmp[caf_tmp[caf_key] != ""]
    saf_tmp = saf_tmp[saf_tmp[saf_key] != ""]

    saf_key_values = set(saf_tmp[saf_key])
    matched_caf = caf_tmp[caf_tmp[caf_key].isin(saf_key_values)].copy()
    matched_caf.to_csv(job_dir / "auto_option4_matched_caf.csv", index=False)

    if matched_caf.empty:
        raise RuntimeError(
            "Option 7 found no overlapping Entry IDs between CAF ternary and SAF ternary outputs."
        )

    merged = matched_caf.merge(
        saf_tmp,
        left_on=caf_key,
        right_on=saf_key,
        how="inner",
        suffixes=("_caf", "_saf"),
    )
    if "saf_source_file" in merged.columns:
        merged = merged.drop(columns=["saf_source_file"])
    merged.to_csv(job_dir / "auto_caf_saf_merged.csv", index=False)

    return [
        "auto_option4_matched_caf.csv",
        "auto_caf_saf_merged.csv",
    ], merge_key_name


def _derive_option7_input_name(job_dir: Path):
    for child in sorted(job_dir.iterdir(), key=lambda p: p.name.lower()):
        if not child.is_dir():
            continue
        if _is_ignored_rel_path(Path(child.name)):
            continue
        if any(cif.suffix.lower() == ".cif" for cif in child.glob("*.cif")):
            return _sanitize_name_component(child.name)
    return "input"


def _zip_option7_outputs(
    job_dir: Path,
    changed_files: list[str],
    run_log_text: str,
    input_name: str,
):
    output_folder = f"7_AUTO_CAF_SAF_{input_name}_output"
    zip_buffer = io.BytesIO()
    written_targets = []

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel in sorted(set(changed_files)):
            rel_path = Path(rel)
            if _is_ignored_rel_path(rel_path):
                continue
            source_path = job_dir / rel_path
            if not source_path.exists() or not source_path.is_file():
                continue
            if rel_path.suffix.lower() == ".cif":
                continue
            target_path = Path(output_folder) / rel_path
            zf.write(source_path, str(target_path))
            written_targets.append(str(target_path))
        zf.writestr(f"{output_folder}/run.log", run_log_text)

    zip_buffer.seek(0)
    changed = written_targets
    changed.append(f"{output_folder}/run.log")
    return zip_buffer, changed


def _find_option8_input_csv(job_dir: Path):
    candidates = []
    for path in job_dir.rglob("*.csv"):
        rel_path = path.relative_to(job_dir)
        if _is_ignored_rel_path(rel_path):
            continue
        candidates.append(path)

    if not candidates:
        return None

    priority_tokens = [
        "auto_caf_saf_merged",
        "merged",
        "saf_caf",
        "binary_features",
    ]

    for token in priority_tokens:
        for path in candidates:
            if token in path.name.lower():
                return path

    return sorted(candidates, key=lambda p: str(p).lower())[0]


def _find_structure_column(df: pd.DataFrame):
    lower_map = {str(col).strip().lower(): col for col in df.columns}
    preferred = [
        "structure",
        "structure type",
        "structure_type",
        "structure_caf",
        "structure_saf",
    ]

    for key in preferred:
        if key in lower_map:
            return lower_map[key]

    for col in df.columns:
        name = str(col).strip().lower()
        if "structure" in name:
            return col

    return None


def _derive_option8_input_name(csv_path: Path):
    return _sanitize_name_component(csv_path.stem)


def _prepare_option8_feature_dataframe(df: pd.DataFrame):
    feature_df = df.copy()

    # Drop common identifier/label columns that are not model features.
    drop_cols = []
    for col in feature_df.columns:
        lower = str(col).strip().lower()
        if lower in {"entry", "formula", "structure", "structure type", "structure_type"}:
            drop_cols.append(col)
            continue
        if lower.startswith("entry_") or lower.startswith("formula_"):
            drop_cols.append(col)
            continue
        if lower.startswith("structure_"):
            drop_cols.append(col)
            continue

    if drop_cols:
        feature_df = feature_df.drop(columns=drop_cols, errors="ignore")

    # Keep only numeric columns; non-numeric strings like element symbols break scaling.
    numeric_df = feature_df.select_dtypes(include=["number"]).copy()
    return numeric_df


def _run_option8_performance(job_dir: Path):
    core_dir = PERFORMANCE_APP_ROOT / "core"
    if not core_dir.exists():
        raise FileNotFoundError(
            f"Could not find performance core modules at {core_dir}. "
            "Ensure the core/ directory exists in the CAF-SAF-gui repository."
        )

    input_csv = _find_option8_input_csv(job_dir)
    if input_csv is None:
        raise RuntimeError(
            "Option 8 requires at least one CSV file upload with feature columns "
            "and a structure label column."
        )

    df = pd.read_csv(input_csv)
    if df.empty:
        raise RuntimeError("Option 8 input CSV is empty.")

    structure_col = _find_structure_column(df)
    if structure_col is None:
        raise RuntimeError(
            "Option 8 requires a structure label column (e.g. Structure, "
            "Structure type, Structure_caf, Structure_saf)."
        )

    labels = df[structure_col].astype(str).str.strip()
    if labels.empty:
        raise RuntimeError("Option 8 structure label column has no usable values.")
    if (labels == "").any():
        raise RuntimeError(
            "Option 8 structure label column contains blank values. "
            "Please fill or remove blank-labeled rows."
        )

    feature_df = _prepare_option8_feature_dataframe(df)
    if feature_df.empty or feature_df.shape[1] == 0:
        raise RuntimeError(
            "Option 8 could not find numeric feature columns after removing "
            "identifier/string columns."
        )

    perf_root = job_dir / "option8_performance_workspace"
    data_dir = perf_root / "data"
    csv_dir = perf_root / "outputs" / "USER"
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)

    copied_csv = csv_dir / input_csv.name
    feature_df.to_csv(copied_csv, index=False)
    pd.DataFrame({"Structure": labels}).to_csv(data_dir / "features.csv", index=False)

    runner_code = "\n".join(
        [
            "import time",
            "import pandas as pd",
            "from sklearn.cross_decomposition import PLSRegression",
            "from sklearn.preprocessing import LabelEncoder",
            "from core import preprocess, report",
            "from core.models import PLS_DA, SVM, PLS_DA_plot, my_xgboost",
            f"csv_file_path = r'{copied_csv}'",
            f"y_path = r'{data_dir / 'features.csv'}'",
            "df_SAF = pd.read_csv(y_path)",
            "y = df_SAF['Structure']",
            "if len(y) != len(pd.read_csv(csv_file_path)):\n    raise RuntimeError('Option 8 expects one structure label per input row.')",
            "encoder = LabelEncoder()",
            "y_encoded = encoder.fit_transform(y)",
            "start_time = time.perf_counter()",
            "X_df, X, columns = preprocess.prepare_standarlize_X_block_(csv_file_path)",
            "print(f'Processing {csv_file_path} with {X.shape[1]} features.')",
            "print('(1/4) Running SVM model...')",
            "svm_report = SVM.get_report(X, y)",
            "report.record_model_performance(svm_report, 'SVM', csv_file_path)",
            "print('(2/4) Running PLS_DA n=2...')",
            "PLS_DA_plot.plot_two_component(X, y, csv_file_path)",
            "print('(3/4) Running PLS_DA model with the best n...')",
            "best_n_components = PLS_DA.find_best_n_dim(X, y_encoded, csv_file_path)",
            "best_pls = PLSRegression(n_components=best_n_components)",
            "pls_report = PLS_DA.generate_classification_report(X, y, best_pls)",
            "report.record_model_performance(pls_report, 'PLS_DA', csv_file_path)",
            "PLS_DA.save_feature_importance(X, columns, y_encoded, best_pls, best_n_components, csv_file_path)",
            "print('(4/4) Running XGBoost model...')",
            "xgb_report = my_xgboost.run_XGBoost(X_df, y)",
            "report.record_model_performance(xgb_report, 'XGBoost', csv_file_path)",
            "my_xgboost.plot_XGBoost_feature_importance(X_df, y_encoded, csv_file_path)",
            "elapsed_time = time.perf_counter() - start_time",
            "print(f'===========Elapsed time: {elapsed_time:0.2f} seconds===========')",
        ]
    )

    env = os.environ.copy()
    current_pythonpath = env.get("PYTHONPATH", "")
    prefix = str(PERFORMANCE_APP_ROOT)
    env["PYTHONPATH"] = (
        f"{prefix}{os.pathsep}{current_pythonpath}" if current_pythonpath else prefix
    )

    process = subprocess.run(
        [sys.executable, "-c", runner_code],
        cwd=str(job_dir),
        capture_output=True,
        text=True,
        env=env,
    )

    if process.returncode != 0:
        raise RuntimeError(
            "Option 8 performance run failed.\n"
            f"STDOUT:\n{process.stdout}\n\nSTDERR:\n{process.stderr}"
        )

    changed_files = []
    outputs_root = perf_root / "outputs"
    for path in outputs_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(job_dir)
        if _is_ignored_rel_path(rel):
            continue
        if path == copied_csv:
            continue
        changed_files.append(str(rel))

    changed_files.sort()
    if not changed_files:
        raise RuntimeError("Option 8 completed but produced no output files.")

    return changed_files, process.stdout, process.stderr, input_csv


def _zip_option8_outputs(
    job_dir: Path,
    changed_files: list[str],
    run_log_text: str,
    input_name: str,
):
    output_folder = f"8_PERFORMANCE_{input_name}_output"
    zip_buffer = io.BytesIO()
    written_targets = []

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel in sorted(set(changed_files)):
            rel_path = Path(rel)
            if _is_ignored_rel_path(rel_path):
                continue
            source_path = job_dir / rel_path
            if not source_path.exists() or not source_path.is_file():
                continue

            parts = rel_path.parts
            if "outputs" in parts:
                idx = parts.index("outputs")
                suffix_parts = list(parts[idx + 1 :]) if idx + 1 < len(parts) else []

                # Drop scaffolding directory from Option 8 workspace.
                if suffix_parts and suffix_parts[0].lower() == "user":
                    suffix_parts = suffix_parts[1:]

                # Performance app writes files under model/input_name/*.
                # Drop the repeated input_name folder to keep paths concise.
                if len(suffix_parts) >= 3 and suffix_parts[1] == input_name:
                    suffix_parts = [suffix_parts[0]] + suffix_parts[2:]

                suffix_path = Path(*suffix_parts) if suffix_parts else Path(rel_path.name)
            else:
                suffix_path = rel_path

            target_path = Path(output_folder) / suffix_path
            zf.write(source_path, str(target_path))
            written_targets.append(str(target_path))

        zf.writestr(f"{output_folder}/run.log", run_log_text)

    zip_buffer.seek(0)
    changed = written_targets
    changed.append(f"{output_folder}/run.log")
    return zip_buffer, changed


def _zip_option1_outputs(
    job_dir: Path,
    changed_files: list[str],
    run_log_text: str,
    input_name: str,
):
    output_folder = f"1_CAF_{input_name}_output"
    zip_buffer = io.BytesIO()
    written_targets = []

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel in sorted(changed_files):
            rel_path = Path(rel)
            if _is_ignored_rel_path(rel_path):
                continue
            source_path = job_dir / rel_path
            if not source_path.exists() or not source_path.is_file():
                continue
            target_rel = rel_path.with_name(f"CAF_{rel_path.name}")
            target_path = Path(output_folder) / target_rel
            zf.write(source_path, str(target_path))
            written_targets.append(str(target_path))
        zf.writestr(f"{output_folder}/run.log", run_log_text)

    zip_buffer.seek(0)
    changed = written_targets
    changed.append(f"{output_folder}/run.log")
    return zip_buffer, changed


def _zip_caf_option_outputs(
    job_dir: Path,
    changed_files: list[str],
    run_log_text: str,
    option: int,
    input_name: str,
):
    output_folder = f"{option}_CAF_{input_name}_output"
    zip_buffer = io.BytesIO()
    written_targets = []

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel in sorted(changed_files):
            rel_path = Path(rel)
            if _is_ignored_rel_path(rel_path):
                continue
            source_path = job_dir / rel_path
            if not source_path.exists() or not source_path.is_file():
                continue
            target_rel = rel_path.with_name(f"CAF_{rel_path.name}")
            target_path = Path(output_folder) / target_rel
            zf.write(source_path, str(target_path))
            written_targets.append(str(target_path))
        zf.writestr(f"{output_folder}/run.log", run_log_text)

    zip_buffer.seek(0)
    changed = written_targets
    changed.append(f"{output_folder}/run.log")
    return zip_buffer, changed


def _is_ignored_rel_path(rel_path: Path):
    return any(part == "__MACOSX" or part.startswith(".") for part in rel_path.parts)


def _zip_saf_outputs(job_dir: Path, before_snapshot: dict, run_log_text: str):
    after_snapshot = _snapshot_files(job_dir)

    export_pairs = []
    used_targets = set()

    for rel_path_str, meta in after_snapshot.items():
        if rel_path_str in before_snapshot and before_snapshot[rel_path_str] == meta:
            continue

        rel_path = Path(rel_path_str)
        if _is_ignored_rel_path(rel_path):
            continue
        if rel_path.suffix.lower() != ".csv":
            continue
        if "csv" not in rel_path.parts:
            continue

        csv_idx = rel_path.parts.index("csv")
        if csv_idx == 0:
            continue

        source_folder_name = rel_path.parts[csv_idx - 1]
        output_folder = f"6_SAF_{source_folder_name}_output"
        base_target = Path(output_folder) / f"structural_{rel_path.name}"
        target = base_target
        duplicate_count = 2
        while str(target) in used_targets:
            target = (
                Path(output_folder)
                / f"structural_{rel_path.stem}_{duplicate_count}{rel_path.suffix}"
            )
            duplicate_count += 1

        used_targets.add(str(target))
        export_pairs.append((job_dir / rel_path, target))

    if not export_pairs:
        raise RuntimeError(
            "No SAF CSV outputs were produced. "
            "Ensure the uploaded ZIP contains valid CIF files."
        )

    output_folders = sorted({str(target.parent) for _, target in export_pairs})

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for source_path, target_path in sorted(export_pairs, key=lambda x: str(x[1])):
            zf.write(source_path, str(target_path))
        for folder in output_folders:
            zf.writestr(f"{folder}/run.log", run_log_text)

    changed = [
        f"{source_path.relative_to(job_dir)} -> {target_path}"
        for source_path, target_path in export_pairs
    ]
    changed.extend([f"{folder}/run.log" for folder in output_folders])
    zip_buffer.seek(0)
    return zip_buffer, changed


def _extract_zip_if_present(uploaded_zip, target_dir: Path):
    if not uploaded_zip or uploaded_zip.filename == "":
        return
    zip_path = target_dir / uploaded_zip.filename
    uploaded_zip.save(zip_path)
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            member_path = Path(member.filename)
            if member_path.is_absolute() or ".." in member_path.parts:
                continue
            if _is_ignored_rel_path(member_path):
                continue
            zf.extract(member, target_dir)


def _save_uploaded_files(files, target_dir: Path):
    for f in files:
        if f and f.filename:
            f.save(target_dir / f.filename)


def _run_caf_option(option: int, job_dir: Path):
    if option == 1:
        filter.run_filter_option(str(job_dir))
    elif option == 2:
        sort.run_sort_option(str(job_dir))
    elif option == 3:
        feature.run_feature_option(str(job_dir))
    elif option == 4:
        match.run_match_option(str(job_dir))
    elif option == 5:
        merge.run_merge_option(str(job_dir))
    else:
        raise ValueError("CAF option must be between 1 and 5")


def _find_cif_dirs(job_dir: Path):
    cif_dirs = set()
    for cif in job_dir.rglob("*.cif"):
        rel = cif.relative_to(job_dir)
        if _is_ignored_rel_path(rel):
            continue
        cif_dirs.add(cif.parent)
    return sorted(cif_dirs)


def _load_saf_main_module():
    configured_path = os.environ.get("STRUCTURE_APP_MAIN", "").strip()

    candidates = []
    if configured_path:
        candidates.append(Path(configured_path))

    candidates.extend(
        [
            BASE_DIR.parent / "structure-analyzer-featurizer-app" / "main.py",
            BASE_DIR / "structure-analyzer-featurizer-app" / "main.py",
        ]
    )

    for parent in BASE_DIR.parents:
        candidates.append(parent / "structure-analyzer-featurizer-app" / "main.py")

    seen = set()
    unique_candidates = []
    for candidate in candidates:
        resolved = candidate.resolve(strict=False)
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        unique_candidates.append(candidate)

    structure_app_main = next((p for p in unique_candidates if p.exists()), None)
    if structure_app_main is None:
        searched = "\n- " + "\n- ".join(str(p) for p in unique_candidates)
        raise FileNotFoundError(
            "Could not find SAF app main.py in any expected location. "
            "Set STRUCTURE_APP_MAIN or keep structure-analyzer-featurizer-app as a sibling. "
            f"Searched:{searched}"
        )

    structure_app_root = structure_app_main.parent

    # SAF main imports modules like `core` relative to its own repository.
    saf_root = str(structure_app_root)
    inserted_path = False
    if saf_root not in sys.path:
        sys.path.insert(0, saf_root)
        inserted_path = True

    spec = importlib.util.spec_from_file_location("saf_main", structure_app_main)
    if spec is None or spec.loader is None:
        raise ImportError("Could not load SAF main module")
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
        return module
    finally:
        if inserted_path and sys.path and sys.path[0] == saf_root:
            sys.path.pop(0)


def _run_saf_with_installed_packages(job_dir: Path):
    def _load_saf_generators():
        import_errors = []
        module_candidates = [
            "SAF.features.generator",
            "saf.features.generator",
            "composition_analyzer_featurizer.SAF.features.generator",
            "composition_analyzer_featurizer.saf.features.generator",
        ]

        for module_name in module_candidates:
            try:
                module = importlib.import_module(module_name)
                binary = getattr(module, "compute_binary_features")
                ternary = getattr(module, "compute_ternary_features")
                quaternary = getattr(module, "compute_quaternary_features")
                return binary, ternary, quaternary
            except Exception as exc:
                import_errors.append(f"{module_name}: {exc}")

        raise RuntimeError(
            "Could not import SAF feature generators from installed packages. "
            "Tried modules:\n- "
            + "\n- ".join(import_errors)
        )
    
    class TimeoutError(Exception):
        """Custom timeout exception."""
        pass

    @contextlib.contextmanager
    def timeout_context(seconds):
        """Context manager to timeout code block."""
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Operation timed out after {seconds} seconds")
        
        # Set the signal handler and alarm
        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(seconds)
        try:
            yield
        finally:
            # Disable the alarm
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

    try:
        from pandas import DataFrame
        (
            compute_binary_features,
            compute_ternary_features,
            compute_quaternary_features,
        ) = _load_saf_generators()
    except Exception as exc:
        raise RuntimeError(
            "SAF fallback runner could not initialize from installed "
            "structure-analyzer-featurizer package."
        ) from exc

    cif_dirs = _find_cif_dirs(job_dir)
    if not cif_dirs:
        raise RuntimeError("No .cif files found. Upload a ZIP containing CIF files.")

    for cif_dir in cif_dirs:
        file_paths = sorted(cif_dir.rglob("*.cif"))
        binary_data = []
        ternary_data = []
        quaternary_data = []
        universal_data = []

        for file_path in file_paths:
            file_path_str = str(file_path)
            matched = False
            try:
                with timeout_context(15):  # 15 seconds per file
                    features, uni_features = compute_binary_features(file_path_str)
                    binary_data.append(features)
                    universal_data.append(uni_features)
                    matched = True
            except (TimeoutError, Exception):
                pass

            if matched:
                continue

            try:
                with timeout_context(15):  # 15 seconds per file
                    features, uni_features = compute_ternary_features(file_path_str)
                    ternary_data.append(features)
                    universal_data.append(uni_features)
                    matched = True
            except (TimeoutError, Exception):
                pass

            if matched:
                continue

            try:
                with timeout_context(15):  # 15 seconds per file
                    features, uni_features = compute_quaternary_features(file_path_str)
                    quaternary_data.append(features)
                    universal_data.append(uni_features)
            except (TimeoutError, Exception):
                continue

        csv_folder_path = cif_dir / "csv"
        csv_folder_path.mkdir(parents=True, exist_ok=True)

        if binary_data:
            DataFrame(binary_data).round(3).to_csv(
                csv_folder_path / "binary_features.csv", index=False
            )
        if ternary_data:
            DataFrame(ternary_data).round(3).to_csv(
                csv_folder_path / "ternary_features.csv", index=False
            )
        if quaternary_data:
            DataFrame(quaternary_data).round(3).to_csv(
                csv_folder_path / "quaternary_features.csv", index=False
            )
        if universal_data:
            DataFrame(universal_data).round(3).to_csv(
                csv_folder_path / "universal_features.csv", index=False
            )


def _run_saf_option(job_dir: Path):
    cif_dirs = _find_cif_dirs(job_dir)
    if not cif_dirs:
        raise RuntimeError("No .cif files found. Upload a ZIP containing CIF files.")

    try:
        saf_main = _load_saf_main_module()
        for cif_dir in cif_dirs:
            saf_main.process_cifs(str(cif_dir))
    except FileNotFoundError:
        _run_saf_with_installed_packages(job_dir)


def _default_answers_for_option(option: int):
    # These defaults intentionally stop optional extra prompts.
    if option == 1:
        return "2\n1\nn"
    if option == 2:
        return "1\n1"
    if option == 3:
        return "1\nn"
    if option == 4:
        return "1\n1\n1"
    if option == 5:
        return "1\n1\n2\n1"
    if option == 7:
        return ""
    if option == 8:
        return ""
    return ""


@app.get("/")
def index():
    return render_template(
        "index.html",
        default_answers={
            1: _default_answers_for_option(1),
            2: _default_answers_for_option(2),
            3: _default_answers_for_option(3),
            4: _default_answers_for_option(4),
            5: _default_answers_for_option(5),
            7: _default_answers_for_option(7),
            8: _default_answers_for_option(8),
        },
    )


@app.post("/run")
def run_job():
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    job_id = f"job_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    option = int(request.form.get("option", "1"))
    prompt_answers = request.form.get("prompt_answers", "").strip()
    if not prompt_answers and option in {1, 2, 3, 4, 5}:
        prompt_answers = _default_answers_for_option(option)

    uploaded_zip = request.files.get("cif_zip")
    extra_files = request.files.getlist("extra_files")

    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()

    try:
        @after_this_request
        def _cleanup_job_dir(response):
            shutil.rmtree(job_dir, ignore_errors=True)
            return response

        _extract_zip_if_present(uploaded_zip, job_dir)
        _save_uploaded_files(extra_files, job_dir)

        before_snapshot = _snapshot_files(job_dir)

        with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(
            stderr_buffer
        ):
            if option in {1, 2, 3, 4, 5}:
                answer_queue = AnswerQueue(prompt_answers)
                with patched_prompts(answer_queue):
                    _run_caf_option(option, job_dir)
            elif option == 6:
                _run_saf_option(job_dir)
            elif option == 7:
                if not uploaded_zip or not uploaded_zip.filename:
                    raise RuntimeError(
                        "Option 7 requires a .zip upload containing .cif files."
                    )

                # Option 1: force defaults for raw CIF->Excel flow.
                with patched_prompts(AnswerQueue("1\n1\n1\nn")):
                    _run_caf_option(1, job_dir)
                changed_option1 = _changed_files_since_snapshot(job_dir, before_snapshot)

                before_option2 = _snapshot_files(job_dir)
                option2_answers, _ = _build_auto_option2_answers(job_dir)
                with patched_prompts(AnswerQueue(option2_answers)):
                    _run_caf_option(2, job_dir)
                changed_option2 = _changed_files_since_snapshot(job_dir, before_option2)

                before_option3 = _snapshot_files(job_dir)
                option3_answers, _ = _build_auto_option3_answers(job_dir)
                with patched_prompts(AnswerQueue(option3_answers)):
                    _run_caf_option(3, job_dir)
                changed_option3 = _changed_files_since_snapshot(job_dir, before_option3)

                before_option6 = _snapshot_files(job_dir)
                _run_saf_option(job_dir)
                changed_option6 = _changed_files_since_snapshot(job_dir, before_option6)

                caf_csv = _pick_option3_caf_csv(job_dir, changed_option3)
                saf_csvs = _pick_option6_saf_csvs(job_dir, changed_option6)

                before_merge = _snapshot_files(job_dir)
                merged_files, merge_key_name = _merge_option3_and_option6_like_option4(
                    job_dir, caf_csv, saf_csvs
                )
                changed_merge = _changed_files_since_snapshot(job_dir, before_merge)

                changed_files = sorted(
                    set(changed_option1)
                    | set(changed_option2)
                    | set(changed_option3)
                    | set(changed_option6)
                    | set(changed_merge)
                    | set(merged_files)
                )
            elif option == 8:
                (
                    changed_files,
                    option8_stdout,
                    option8_stderr,
                    option8_input_csv,
                ) = _run_option8_performance(job_dir)
                stdout_buffer.write(option8_stdout)
                stderr_buffer.write(option8_stderr)
            else:
                raise ValueError("Unsupported option. Use 1-8.")

        if option == 7:
            # Option 7 already collects per-stage changed files.
            pass
        elif option == 8:
            # Option 8 already collects generated output files.
            pass
        elif option == 6:
            # First pass: discover changed output names.
            _, changed_files = _zip_saf_outputs(
                job_dir, before_snapshot, ""
            )
        elif option == 1:
            # First pass: discover changed output names.
            _, changed_files = _zip_outputs(job_dir, before_snapshot, "")
        else:
            # First pass: discover changed output names.
            _, changed_files = _zip_outputs(job_dir, before_snapshot, "")

        option1_prefix_msg = ""
        option2_prefix_msg = ""
        option7_prefix_msg = ""
        run_message = "Run completed successfully."
        if option == 1:
            option1_input_name = _derive_option1_input_name(changed_files)
            option1_prefix_msg = (
                f"Option 1: Formulas from {option1_input_name} filtered and "
                "periodic table heatmap generated\n"
                "Proceed to option 2 and upload the file ending with "
                "_sorted.xlsx\n\n"
            )
            run_message = (
                f"Option 1 complete: formulas from {option1_input_name} filtered and "
                "periodic table heatmap generated. Proceed to option 2 and upload "
                "the file ending with _sorted.xlsx."
            )
        if option == 2:
            sorted_file_name = _derive_option2_generated_filename(changed_files)
            option2_prefix_msg = (
                f"Option 2: Sorted file generated: {sorted_file_name}\n"
                "Proceed to option 3 and use this file.\n\n"
            )
            run_message = (
                f"Option 2 complete: sorted file generated ({sorted_file_name}). "
                "Proceed to option 3 and use this file."
            )
        if option == 3:
            run_message = (
                "Option 3 complete: compositional feature files generated in "
                "3_CAF_*_output."
            )
        if option == 4:
            run_message = (
                "Option 4 complete: CIF/Excel matching outputs generated in "
                "4_CAF_*_output."
            )
        if option == 5:
            run_message = (
                "Option 5 complete: merged outputs generated in 5_CAF_*_output."
            )
        if option == 6:
            run_message = (
                "Option 6 complete: structural feature outputs generated in "
                "6_SAF_*_output."
            )
        if option == 7:
            option7_prefix_msg = (
                "Option 7: Automatic CAF+SAF completed.\n"
                "Matched file (auto_option4_matched_caf.csv): CAF ternary rows "
                "with Entry IDs also present in SAF ternary.\n"
                "Merged file (auto_caf_saf_merged.csv): inner-join of CAF ternary "
                "and SAF ternary on shared Entry IDs.\n\n"
            )
            run_message = (
                "Option 7 complete: auto-ran 1->2->3->6 with defaults. "
                "Matched = CAF rows with shared Entry IDs; "
                "Merged = CAF+SAF inner-join on Entry."
            )
        if option == 8:
            run_message = (
                "Option 8 complete: SAF-CAF performance models ran "
                "(SVM, PLS-DA, XGBoost) and generated feature-performance outputs."
            )

        run_log_text = (
            option1_prefix_msg
            + option2_prefix_msg
            + option7_prefix_msg
            + (
                "Option 7 notes:\n"
                "- Requires a .zip containing .cif files.\n"
                "- Uses default options for CAF 1 -> 2 -> 3 and SAF 6.\n"
                "- Performs an option-4-style match/merge between CAF(3) and SAF(6).\n"
                + (f"- Merge key used: {merge_key_name}\n" if option == 7 else "")
                + "\n"
                if option == 7
                else ""
            )
            + "=== STDOUT ===\n"
            + stdout_buffer.getvalue()
            + "\n=== STDERR ===\n"
            + stderr_buffer.getvalue()
            + "\n=== OUTPUT FILES ===\n"
            + "\n".join(changed_files)
        )

        if option == 6:
            output_zip_buffer, _ = _zip_saf_outputs(
                job_dir, before_snapshot, run_log_text
            )
        elif option == 1:
            output_zip_buffer, _ = _zip_option1_outputs(
                job_dir,
                changed_files,
                run_log_text,
                _derive_option1_input_name(changed_files),
            )
        elif option in {2, 3, 4, 5}:
            output_zip_buffer, _ = _zip_caf_option_outputs(
                job_dir,
                changed_files,
                run_log_text,
                option,
                _derive_caf_input_name(changed_files),
            )
        elif option == 7:
            output_zip_buffer, _ = _zip_option7_outputs(
                job_dir,
                changed_files,
                run_log_text,
                _derive_option7_input_name(job_dir),
            )
        elif option == 8:
            output_zip_buffer, _ = _zip_option8_outputs(
                job_dir,
                changed_files,
                run_log_text,
                _derive_option8_input_name(option8_input_csv),
            )
        else:
            output_zip_buffer, _ = _zip_outputs(
                job_dir, before_snapshot, run_log_text
            )

        response = send_file(
            output_zip_buffer,
            as_attachment=True,
            download_name=f"{job_id}_outputs.zip",
            mimetype="application/zip",
        )
        response.headers["X-Run-Message"] = run_message
        response.headers["Access-Control-Expose-Headers"] = (
            "Content-Disposition, X-Run-Message"
        )
        return response
    except Exception as exc:
        message = (
            f"Run failed: {exc}\n\n"
            f"STDOUT:\n{stdout_buffer.getvalue()}\n\n"
            f"STDERR:\n{stderr_buffer.getvalue()}"
        )
        return message, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)