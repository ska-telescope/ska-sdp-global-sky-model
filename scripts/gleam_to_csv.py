import csv
from collections import OrderedDict

from astroquery.vizier import Vizier

row_limit = 3


def catalog_to_csv(ordered_dict: OrderedDict, file_path):
    # Open the file in write mode
    keys, values = [], []
    with open(file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        for row in range(0, row_limit + 1):
            for key, value in ordered_dict.items():
                if row == 0:
                    keys.append(key)

                else:
                    values.append(value[row - 1])

            # Write the keys as the first row
            if row == 0:
                writer.writerow(keys)

            # Write the values as subsequent rows

            writer.writerow(values)
            values = []


Vizier.ROW_LIMIT = row_limit
Vizier.columns = ["**"]
key = "VIII/100"
catalog = Vizier.get_catalogs(key)

catalog_to_csv(catalog[1], "gleam.csv")
