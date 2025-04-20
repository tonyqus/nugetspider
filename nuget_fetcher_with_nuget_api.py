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

    def search_packages(self, query, take=10):
        """Search for packages by query."""
        search_url = self.resources.get("SearchQueryService")
        if not search_url:
            raise RuntimeError("SearchQueryService not found in resources.")
        
        params = {"q": query, "take": take}
        response = self.session.get(search_url, params=params)
        response.raise_for_status()
        return response.json()

    def get_package_metadata(self, package_id):
        """Retrieve metadata for a specific package."""
        metadata_url = self.resources.get("RegistrationsBaseUrl")
        if not metadata_url:
            raise RuntimeError("RegistrationsBaseUrl not found in resources.")
        
        url = f"{metadata_url}{package_id}/index.json"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

# Example usage
if __name__ == "__main__":
    fetcher = NuGetFetcher()
    search_results = fetcher.search_packages("json")
    print("Search Results:", search_results)

    package_metadata = fetcher.get_package_metadata("Newtonsoft.Json")
    print("Package Metadata:", package_metadata)
