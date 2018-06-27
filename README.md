## Some Tools to get GBIF data into Specify 6
This is an experimental and was created for a project at UR

#### Dependencies
python 3

pip 3

[pygbif](https://github.com/sckott/pygbif)

[MySQL Connector](https://dev.mysql.com/doc/connector-python/en/)

#### Installation
`pipenv install`

Or

`pip3 install pygbif mysql-connector`

#### Usage

 **To remove occurrence data and fetch all synonyms for species queried from GBIF:**
 _______________________________________________________________________________

1) Download some [species data](https://www.gbif.org/species/search?q=) from GBIF as a csv

2) Run the fetch tool - this will likely run for a long time. To stop it, type ctrl+c:

`python3 specify_csv.py -i species_data.csv -o synonyms.csv`

3) Import the resulting synonyms.csv file into Specify

The mappings are as follows (in the order they're displayed in Specify):

| Data Set Columns | Specify Taxon Import Field |
|------------|----------------------------|
| taxonID    | Species GUID               |
| genus      | Genus                      |
| class      | Class                      |
| authorship | Species Author             |
| order      | Order                      |
| kingdom    | Kingdom                    |
| family     | Family                     |
| phylum     | Phylum                     |
| name       | Species                    |
| source     | Species Source             |

###### NOTE
If you want to use the synonymization tool you need to map the taxonID field to the Species GUID field in Specify

**To synonymize imported GBIF data in Specify:**
__________________________________________

1) Run the synonymization tool with an optional dry run, which will produce two
csv reports (specify_accepted_report.csv and specify_synonyms_report.csv) that
show which Specify records will be set to preferred names and which will become synonyms, respectively.

###### NOTE
The first time you run this tool, you will be prompted to enter the following Specify server credentials:

*database name*

*database username*

*database password*

*database hostname*

This will produce a specify_config.json file that you can use to connect to Specify without re-entering your credentials.

###### NOTE
In order for this to work, you need to use the same synonyms.csv file you used to import your data into Specify

`python3 specify_synonymize.py -i synonyms.csv -d`

The above command will create a config file and produce specify_accepted_report.csv and specify_synonyms_report.csv

2) Run the synonymization tool and update the Specify records outlined in the reports:  

`python3 specify_synonymize.py -i synonyms.csv -c specify_config.json`
