from pygbif import species
from urllib.error import HTTPError


class GBIFSpeciesFetcher(object):
    filtered_fields = [
        'taxonID', 'species', 'genus', 'class', 'rank', 'parent', 'authorship', 'order', 'publishedIn', 'kingdom',
        'family', 'scientificName', 'phylum', 'parentOfTaxon', 'name', 'canonicalName', 'source', 'synonym', 'speciesKey',
    ]

    def __init__(self, species_id, **kwargs):
        self.species_id = species_id
        self.results = []
        self.root_fetched = False
        try:
            self.limit = kwargs['limit']
        except KeyError:
            self.limit = 100  # the default

    def fetch(self, offset):
        if self.root_fetched is False:
            root = species.name_usage(key=self.species_id)
            if not root:
                return None
            else:
                self.results.append(self.parse_result(root))
                self.root_fetched = True
                return self.fetch(0)
        else:
            try:
                result = species.name_usage(self.species_id, offset=offset, data='synonyms', limit=self.limit)
                print(result)
                self.parse_results(result)
                if not result['endOfRecords']:
                    print(offset + self.limit)
                    return self.fetch(offset + self.limit)
            except HTTPError as error:
                print(error)
                return None

    def fetch_all(self):
        return self.fetch(0) if self.species_id else None

    def parse_result(self, result):
        filtered = {}
        filtered['source'] = 'GBIF'
        if result['rank'] in ['SPECIES', 'SUBSPECIES']:
            try:
                split_name = result['canonicalName'].split()
                if result['rank'] == 'SUBSPECIES':
                    filtered['parentOfTaxon'] = "{} {}".format(split_name[0], split_name[1])
                else:
                    filtered['parentOfTaxon'] = split_name[0]
                filtered['name'] = result['canonicalName'].split()[-1]
            except KeyError as error:
                filtered['parentOfTaxon'] = 'test'

            for key in self.filtered_fields:
                try:
                    filtered[key] = result[key]
                    if key is 'taxonID':
                        filtered[key] = result[key].split(':')[-1]
                except KeyError as error:
                    pass
            return filtered
        return None

    def parse_results(self, result):
        results = result['results']
        filtered_results = []
        for _result in results:
            parsed = self.parse_result(_result)
            if parsed is not None:
                filtered_results.append(parsed)
        self.results = self.results + filtered_results

    # Super annoying - can't find a list of all available fields anywhere so I need to hardcode some in
    @staticmethod
    def get_species_data_header(species_id):
        try:
            header = set(species.name_usage(key=species_id).keys())
            header.add('publishedIn')
            header.add('description')
            return header
        except HTTPError as error:
            print(error)
            return None
