import gamla

to_kebab_case = gamla.compose_left(
    str.lower,
    gamla.replace_in_text("_", "-"),
)
