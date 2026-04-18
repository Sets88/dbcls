from visidata import TableSheet


def reference_sheets(right_sheet, field, value):
    rows = []
    for row in right_sheet:
        if getattr(row, field) == value:
            rows.append(row.as_dict())
    if rows:
        return PyobjSheet(f'{right_sheet.name}_reference[{len(rows)}]', source=rows)
    return None
