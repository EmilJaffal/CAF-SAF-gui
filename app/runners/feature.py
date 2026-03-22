import os
import warnings

import click
import pandas as pd
from CAF.features import generator

from app.util import folder

"""
Ignore warnings for Pandas
"""
warnings.simplefilter("ignore")


def _get_column_case_insensitive(df, target_name):
    return next((c for c in df.columns if c.lower() == target_name.lower()), None)


def _normalize_structure_column(df):
    aliases = ["Structure", "Structure type", "Structure_type"]
    for alias in aliases:
        col = _get_column_case_insensitive(df, alias)
        if not col:
            continue
        if col != "Structure":
            df = df.rename(columns={col: "Structure"})
        return df
    return df


def _append_entry_to_feature_files(
    save_dir,
    file_prefix,
    entry_by_formula,
    structure_by_formula,
    add_extended_features,
):
    feature_sets = ["binary", "ternary", "quaternary", "universal"]
    if add_extended_features:
        feature_sets.extend(["binary_ext", "ternary_ext", "universal_ext"])

    for feature_name in feature_sets:
        csv_path = os.path.join(save_dir, f"{file_prefix}_{feature_name}.csv")
        if not os.path.exists(csv_path):
            continue

        feature_df = pd.read_csv(csv_path)
        formula_col = _get_column_case_insensitive(feature_df, "Formula")
        if not formula_col:
            continue

        formula_series = feature_df[formula_col].astype(str).str.strip()
        formula_idx = feature_df.columns.get_loc(formula_col)

        if entry_by_formula:
            entry_values = formula_series.map(entry_by_formula)
            existing_entry_col = _get_column_case_insensitive(feature_df, "Entry")
            if existing_entry_col:
                feature_df[existing_entry_col] = entry_values
            else:
                feature_df.insert(formula_idx, "Entry", entry_values)
                formula_idx += 1

        if structure_by_formula:
            structure_values = formula_series.map(structure_by_formula)
            existing_structure_col = _get_column_case_insensitive(
                feature_df, "Structure"
            )
            if existing_structure_col and existing_structure_col != "Structure":
                feature_df = feature_df.rename(
                    columns={existing_structure_col: "Structure"}
                )
                existing_structure_col = "Structure"

            if existing_structure_col:
                feature_df[existing_structure_col] = structure_values
            else:
                feature_df.insert(formula_idx, "Structure", structure_values)

        feature_df.to_csv(csv_path, index=False)


def run_feature_option(script_dir_path):
    # User select the Excel file
    formula_excel_path = folder.list_xlsx_files_with_formula(script_dir_path)
    if formula_excel_path:
        print(f"Selected Excel file: {formula_excel_path}")
    else:
        print("No Excel file was found. Exiting.")
        return

    # list Excel files containing with "formula" columns
    _, base_name = os.path.split(formula_excel_path)
    base_name_no_ext = os.path.splitext(base_name)[0]
    df = _normalize_structure_column(pd.read_excel(formula_excel_path))
    col = _get_column_case_insensitive(df, "Formula")
    if not col:
        print("No formula column found. Exiting.")
        return

    formulas = df[col]
    entry_col = _get_column_case_insensitive(df, "Entry")
    structure_col = _get_column_case_insensitive(df, "Structure")
    entry_by_formula = {}
    structure_by_formula = {}
    if entry_col:
        entry_map_df = df[[col, entry_col]].dropna(subset=[col, entry_col]).copy()
        entry_map_df[col] = entry_map_df[col].astype(str).str.strip()
        entry_map_df[entry_col] = entry_map_df[entry_col].astype(str).str.strip()
        entry_map_df = entry_map_df[entry_map_df[entry_col] != ""]
        entry_map_df = entry_map_df.drop_duplicates(subset=[col], keep="first")
        entry_by_formula = dict(zip(entry_map_df[col], entry_map_df[entry_col]))

    if structure_col:
        structure_map_df = df[[col, structure_col]].dropna(
            subset=[col, structure_col]
        ).copy()
        structure_map_df[col] = structure_map_df[col].astype(str).str.strip()
        structure_map_df[structure_col] = (
            structure_map_df[structure_col].astype(str).str.strip()
        )
        structure_map_df = structure_map_df[structure_map_df[structure_col] != ""]
        structure_map_df = structure_map_df.drop_duplicates(
            subset=[col], keep="first"
        )
        structure_by_formula = dict(
            zip(structure_map_df[col], structure_map_df[structure_col])
        )

    # User select whether to add normalized compositional one-hot encoding
    # is_encoding_added = click.confirm(
    #     "\nDo you want to include normalized composition vector? "
    #     "(Default is N)",
    #     default=False,
    # )

    # if is_encoding_added:
    #     is_all_element_displayed = click.confirm(
    #         "\nDo you want to include all elements in the composition "
    #         "vector or"
    #         " only the ones present in the dataset? "
    #         "(Default is Y)",
    #         default=True,
    #     )

    add_extended_features = click.confirm(
        "\nDo you want to save additional files containing features with"
        "\nmathematical operations? Ex) +, -, *, /, exp, square, cube, etc."
        "\n(Default is N)",
        default=False,
    )

    generator.get_composition_features(
        formulas,
        extended_features=add_extended_features,
        save_dir=os.path.dirname(formula_excel_path),
        file_prefix=base_name_no_ext,
    )

    if entry_by_formula or structure_by_formula:
        _append_entry_to_feature_files(
            save_dir=os.path.dirname(formula_excel_path),
            file_prefix=base_name_no_ext,
            entry_by_formula=entry_by_formula,
            structure_by_formula=structure_by_formula,
            add_extended_features=add_extended_features,
        )
        click.secho(
            "Entry/Structure columns appended to generated feature CSV files.",
            fg="cyan",
        )
