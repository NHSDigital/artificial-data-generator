# Export directories from Databricks

$importEnv = $(Read-Host -Prompt "Import to 'dev' or 'staging'? (default='dev')")

if ($importEnv -eq "staging")
{
  $workspaceRoot = "/staging"
}
elseif ($importEnv -eq "dev")
{
  $branchName = $(git rev-parse --abbrev-ref HEAD)
  $workspaceRoot = "/data_manager_projects/iuod_dae_test_data_generator/${branchName}"
}
else
{
  "Invalid value: '$importEnv'"
  exit 1;
}

$projects = @(
  "iuod_artificial_data_generator"
  "iuod_artificial_data_admin",
  "artificial_hes",
  "artificial_hes_meta"
)

foreach ( $projectName in $projects )
{
  $localPath = "projects/$projectName"
  $databricksPath = "$workspaceRoot/$projectName"
  $resourceExists = !($(databricks workspace ls $databricksPath) -like '*"error_code":"RESOURCE_DOES_NOT_EXIST"*')

  if ($resourceExists) { 
    "'$($databricksPath)' already exists in the Databricks workspace"
    $overwrite = Read-Host -Prompt "Do you wish to overwrite? (y/n)"
    
    if ($overwrite.ToLower() -eq "y")
    {
      # Require user confirmation of the project name for overwriting
      $confirmMessage = "Confirm the project name to overwrite ($($projectName))"
      $projectNameCheck = Read-Host -Prompt $confirmMessage

      while ($projectNameCheck -ne $projectName) 
      {
        # Keep going until the names match
        "'$($projectNameCheck)' does not match '$($projectName)', please try again"
        $projectNameCheck = Read-Host -Prompt $confirmMessage
      }

      # Import the directory with overwrite
      databricks workspace import_dir -o $localPath $databricksPath
    }
    else {
      continue
    }
  
  } 
  else 
  {
    # Import the directory
    databricks workspace import_dir $localPath $databricksPath
  }
 
}