from travel_etl.core.table import S3JsonFromYDB


class ProdOptions(S3JsonFromYDB):
    queries = ["countries.sql", "num_nights.sql", "dates.sql"]

    def transform(self):
        countries = list(map(lambda x: x["country_name"], self.data[0]))

        num_nights = list(map(lambda x: x["num_nights"], self.data[1]))

        dt = self.data[2][0]

        self.result = {
            "countries": countries,
            "num_nights": num_nights,
            "min_start_date": dt["min_start_date"],
            "max_start_date": dt["max_start_date"],
            "min_end_date": dt["min_end_date"],
            "max_end_date": dt["max_end_date"],
        }
