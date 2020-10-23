import json

class DBInfo:
    DEFAULT_HOST = "127.0.0.1"
    DEFAULT_CONFIG_FILENAME = "specify_config.json"

    def __init__(self, config_file):
        if not config_file:
            self.config = DBInfo.configure()
        else:
            self.config = DBInfo.parse_config(config_file)

    @staticmethod
    def parse_config(config_file):
        try:
            with open(config_file, "r") as config:
                return json.load(config)
        except IOError as error:
            print(error)
            sys.exit()

    @staticmethod
    def configure(**kwargs):
        print("Creating a config file to connect to the Specify database")
        config = {}
        config["database"] = input("Enter the name of your Specify database: ")
        config["user"] = input("Enter your Specify username: ")
        config["password"] = input("Enter your Specify password: ")
        config["host"] = input("Enter the host address for your Specify server: ")
        filepath = "{}/{}{}".format(
            os.path.abspath(os.getcwd()),
            kwargs["config_prefix"] if "config_prefix" in kwargs else "",
            DBInfo.DEFAULT_CONFIG_FILENAME,
        )
        try:
            with open(filepath, "w") as config_file:
                json.dump(config, config_file)
                print("Successfully saved config to {}".format(filepath))
        except IOError as error:
            print(error)
        return config