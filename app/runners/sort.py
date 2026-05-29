import os

import click
import pandas as pd
from bobleesj.utils.parsers.formula import Formula
from bobleesj.utils.sorters.element_sorter import ElementSorter
from bobleesj.utils.sources.oliynyk import Oliynyk
from bobleesj.utils.sources.oliynyk import Property as P

from app.util import folder, prompt


_OVERRIDE_PREFIX = "OVERRIDE_MENDELEEV_"


def _apply_mendeleev_override(oliynyk: Oliynyk):
    """Apply OVERRIDE_MENDELEEV_{ELEMENT}_... env vars and built-in manual updates.

    Env var examples:
      OVERRIDE_MENDELEEV_MN_MN=65
      OVERRIDE_MENDELEEV_SE_SE=87
      OVERRIDE_MENDELEEV_FE_FE=65

    This function first applies any environment-driven overrides (they take
    precedence), then applies a small set of manual defaults requested by the
    user (Se -> 87, Fe -> 65) if they weren't overridden by env vars.
    """
    applied = set()
    try:
        for k, v in os.environ.items():
            if not k.startswith(_OVERRIDE_PREFIX):
                continue
            parts = k.split("_")
            if len(parts) < 3:
                continue
            elem_raw = parts[2]
            elem = elem_raw.title()
            try:
                new_num = float(v) if "." in v else int(v)
            except ValueError:
                print(f"Invalid override value for {elem}: {v}")
                continue
            if elem in oliynyk.db:
                oliynyk.db[elem][P.MEND_NUM] = new_num
                applied.add(elem)
                print(f"Applied Mendeleev override: {elem} -> {new_num}")
            else:
                print(f"Element {elem} not found in Oliynyk DB; skipping override")
    except Exception as e:
        print(f"Failed to apply Mendeleev overrides from env: {e}")

    # Manual updates requested by user (applied when not provided via env)
    manual_updates = {"Se": 87, "Fe": 65}
    for elem, val in manual_updates.items():
        if elem in applied:
            continue
        if elem in oliynyk.db:
            try:
                oliynyk.db[elem][P.MEND_NUM] = val
                print(f"Applied manual Mendeleev update: {elem} -> {val}")
            except Exception as e:
                print(f"Failed to apply manual update for {elem}: {e}")


def run_sort_option(script_dir_path):
    sort_method = prompt.choose_sort_method()
    if sort_method in [1, 2, 3, 4]:
        formula_excel_path = folder.list_xlsx_files_with_formula(
            script_dir_path
        )
        if formula_excel_path:
            print(f"You've selected: {formula_excel_path}")
    dir_path, base_name = os.path.split(formula_excel_path)
    excel_filename = os.path.splitext(base_name)[0]
    df = pd.read_excel(formula_excel_path)
    # Read the Formula or formula column into a list of formulas
    formulas = _extract_formulas(df)
    if sort_method == 1:
        _run_sort_by_custom_label(formulas, df, dir_path, excel_filename)
    elif sort_method == 2:
        _run_sort_by_stoichiometry(formulas, df, dir_path, excel_filename)
    elif sort_method == 3:
        _run_sort_by_property(formulas, df, dir_path, excel_filename)
    elif sort_method == 4:
        _run_sort_by_mendeleev_number(formulas, df, dir_path, excel_filename)


def _extract_formulas(df):
    if "Formula" in df.columns:
        return df["Formula"].tolist()
    elif "formula" in df.columns:
        return df["formula"].tolist()
    else:
        raise ValueError(
            "No 'Formula' or 'formula' column found in the Excel file."
        )


def _save_and_update(df, formulas_sorted, dir_path, filename):
    # Prefer to overwrite the original Formula column so that the sorted
    # representation is immediately visible in the same column users expect.
    if "Formula" in df.columns:
        df["Formula"] = formulas_sorted
    elif "formula" in df.columns:
        df["formula"] = formulas_sorted
    else:
        df["Sorted Formula"] = formulas_sorted

    _save_sorted_to_excel(df, dir_path, filename)


def _run_sort_by_custom_label(formulas, df, dir_path, filename):
    element_sorter = ElementSorter(excel_path="data/sort/custom-labels.xlsx")
    formulas_sorted = [
        Formula(formula).sort_by_custom_label(element_sorter.label_mapping)
        for formula in formulas
    ]
    filename = f"{filename}_by_custom_label"
    _save_and_update(df, formulas_sorted, dir_path, filename)


def _run_sort_by_stoichiometry(formulas, df, dir_path, filename):
    is_ascending, is_normalized = _ask_ascending_normalize()
    oliynyk = Oliynyk()
    _apply_mendeleev_override(oliynyk)
    formulas_sorted = [
        Formula(formula).sort_by_stoichiometry(
            oliynyk.get_property_data_for_formula(formula, P.MEND_NUM),
            ascending=is_ascending,
            normalize=is_normalized,
        )
        for formula in formulas
    ]
    filename = _add_suffixes(
        filename + "_by_stoichiometry", is_ascending, is_normalized
    )
    _save_and_update(df, formulas_sorted, dir_path, filename)


def _run_sort_by_property(formulas, df, dir_path, filename):
    selected_property = P.select()
    oliynyk = Oliynyk()
    _apply_mendeleev_override(oliynyk)
    is_ascending, is_normalized = _ask_ascending_normalize()
    formulas_sorted = [
        Formula(formula).sort_by_elemental_property(
            oliynyk.get_property_data_for_formula(formula, selected_property),
            ascending=is_ascending,
            normalize=is_normalized,
        )
        for formula in formulas
    ]
    filename = f"{filename}_by_property_{selected_property.name}"
    filename = _add_suffixes(filename, is_ascending, is_normalized)
    _save_and_update(df, formulas_sorted, dir_path, filename)


def _run_sort_by_mendeleev_number(formulas, df, dir_path, filename):
    oliynyk = Oliynyk()
    _apply_mendeleev_override(oliynyk)
    is_ascending, is_normalized = _ask_ascending_normalize()
    formulas_sorted = [
        Formula(formula).sort_by_elemental_property(
            oliynyk.get_property_data_for_formula(formula, P.MEND_NUM),
            ascending=is_ascending,
            normalize=is_normalized,
        )
        for formula in formulas
    ]
    filename = f"{filename}_by_mendeleev_number"
    filename = _add_suffixes(filename, is_ascending, is_normalized)
    _save_and_update(df, formulas_sorted, dir_path, filename)


def _add_suffixes(filename, is_ascending, is_normalized, method=None):
    if method:
        filename += "_" + method
    if not is_ascending:
        filename += "_descend"
    if is_normalized:
        filename += "_norm"
    return filename


def _ask_ascending_normalize():
    is_ascending = _ascend_order()
    is_normalized = _normalize_formula()
    return is_ascending, is_normalized


def _save_sorted_to_excel(df, dir_path, filename):
    output_path = os.path.join(dir_path, f"{filename}.xlsx")
    df.to_excel(output_path, index=False)
    print(f"Sorted formulas saved to {output_path}")


def _ascend_order():
    is_ascending_order = click.confirm(
        "\nWould you like to sort the indices in ascending order? "
        "(Default is Y)",
        default=True,
    )
    return is_ascending_order


def _normalize_formula():
    is_indices_as_fractions = click.confirm(
        "\nWould you like to convert indices into fractions? (Default is N)",
        default=False,
    )
    return is_indices_as_fractions
