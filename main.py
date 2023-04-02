from travel_etl.core.table import Table

if __name__ == "__main__":
    
    table = Table("parser")
    print("target={target}".format(target=table))