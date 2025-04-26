import requests

class NuGetFetcher:
    BASE_URL = "https://api.nuget.org/v3/index.json"

    def __init__(self):
        self.session = requests.Session()
        self.resources = self._get_resources()

    def _get_resources(self):
        """Fetch the available resources from the NuGet API."""
        response = self.session.get(self.BASE_URL)
        response.raise_for_status()
        return {res['@type']: res['@id'] for res in response.json()['resources']}

    def search_packages(self, query="", take=10, skip=0, sort_by="totalDownloads"):
        """Search for packages by query with sorting and pagination."""
        search_url = self.resources.get("SearchQueryService")
        if not search_url:
            raise RuntimeError("SearchQueryService not found in resources.")
        
        params = {
            "q": query,
            "take": take,
            "skip": skip,
            "sortBy": sort_by
        }
        response = self.session.get(search_url, params=params)
        response.raise_for_status()
        return response.json()

    def get_top_downloaded_packages(self, top_n=5000, batch_size=100):
        """Retrieve the top downloaded packages."""
        all_packages = []
        for skip in range(0, top_n, batch_size):
            result = self.search_packages(query="", take=batch_size, skip=skip, sort_by="totalDownloads")
            all_packages.extend(result.get("data", []))
            if len(result.get("data", [])) < batch_size:
                break  # Stop if fewer results are returned than requested
        return all_packages

# Example usage
if __name__ == "__main__":
    fetcher = NuGetFetcher()
    
    # Fetch the top 10,000 downloaded packages
    top_packages = fetcher.get_top_downloaded_packages(top_n=10000, batch_size=100)
    print(f"Retrieved {len(top_packages)} packages.")
    for package in top_packages[:10]:  # Print the first 10 packages as a sample
        print(f"Package ID: {package['id']}, Downloads: {package['totalDownloads']}")