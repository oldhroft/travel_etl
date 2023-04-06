from travel_etl.det.pivot import DetPivot

if __name__ == "__main__":
    table = DetPivot("test", "test_table")
    table.create_table()
