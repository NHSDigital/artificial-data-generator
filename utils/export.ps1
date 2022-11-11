# Export directories from Databricks

$exportEnv = $(Read-Host -Prompt "Export 'dev' or 'staging' folders? (default=dev)")

if ($exportEnv -eq "staging")
{
  $workspaceRoot = "/staging"
}
elseif ($exportEnv -eq "dev")
{
  $branchName = $(git rev-parse --abbrev-ref HEAD)
  $workspaceRoot = "/data_manager_projects/iuod_dae_test_data_generator/${branchName}"
}
else
{
  "Invalid value: '$exportEnv'"
  exit 1;
}

$projects = @(
  "iuod_artificial_data_generator",
  "iuod_artificial_data_admin",
  "artificial_hes",
  "artificial_hes_meta"
)

git stash  # Stash local changes before overwrite

foreach ( $projectName in $projects )
{
  $databricksPath = "$workspaceRoot/$projectName"
  $localPath = "projects/$projectName"
  Remove-Item $localPath -Recurse
  databricks workspace export_dir -o $databricksPath $localPath
}