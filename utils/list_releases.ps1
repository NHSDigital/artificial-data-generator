# List the latest code promotion project releases

$workspaceRoot = "/Users/admin/releases/code-promotion"
$projects = @(
  "iuod_artificial_data_generator"
  "iuod_artificial_data_admin",
  "artificial_hes",
  "artificial_hes_meta"
)

foreach ( $projectName in $projects )
{
  $projectReleasesRoot = "$workspaceRoot/$projectName"

  # Get all the releases for the project
  $releases = $(databricks workspace ls $projectReleasesRoot)

  # Find the latest release
  $latestRelease = $null
  $latestTimestamp = [datetime]"1900-01-01"
  foreach ($release in $releases)
  {
    $releaseSegments = $release.Split(".")
    $releaseIdTimestamp = $releaseSegments[0].Split("+")
    $releaseId = $releaseIdTimestamp[0]  # Not used
    $releaseTimestamp = [datetime]::ParseExact($releaseIdTimestamp[1], "yyyyMMddHHmmss", $null)
    $releaseHash = $releaseSegments[1].Substring(3)  # Not used

    if ($latestTimestamp -lt $releaseTimestamp)
    {
      # Update the latest release
      $latestRelease = $release
      $latestTimestamp = $releaseTimestamp
    }

  } 

  "Latest release for '$projectName': '$latestRelease' on $($latestTimestamp.ToString("yyyy-MM-dd 'at' HH:mm:ss"))"
}