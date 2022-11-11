import pathlib

# Projects requiring docs
projects = ["artificial_hes"]

# Constant paths
this_path = pathlib.Path(__file__)
template_path = this_path.parent / "create_user_docs_template.py"
docs_path = this_path.parent.parent
projects_root = this_path.parent.parent.parent / "projects"

def replace_template_variable(template: str, token_value: str, new_value: str) -> str:
  filled_template = template.replace("".join(["{{", token_value, "}}"]), new_value)
  return filled_template

for project_name in projects:
  project_path = projects_root / project_name
  notebook_path = project_path / "schemas" / "create_user_docs.py"

  # Read the template content
  with template_path.open("r") as template_file:
    notebook_content = template_file.read()

  # Populate placeholders in the template
  for doc_file_path in docs_path.glob("**/*.md"):
    with doc_file_path.open("r") as doc_file:
      doc_file_content = doc_file.read()

    notebook_content = replace_template_variable(notebook_content, doc_file_path.name, doc_file_content)
    notebook_content = replace_template_variable(notebook_content, "database_name", project_name)

  # Write the full content
  with notebook_path.open("w+") as notebook_file:
    notebook_file.write(notebook_content)
