def clean_column_name(name):
    return name.strip().lower().replace(" ", "_").replace("-", "_")