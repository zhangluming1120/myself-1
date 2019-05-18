import pandas as pd

def main():
    totals = pd.read_csv('totals.csv').set_index(keys=['name'])
    totals_each_city = totals.sum(axis = 0)
    counts = pd.read_csv('counts.csv').set_index(keys=['name'])
    counts_each_city = counts.sum(axis = 0)
    # print(format(totals.sum(axis = 1)))

    print("City with lowest total precipitation: \n{}".format(totals.sum(axis = 1).idxmin()))
    print("Average precipitation in each month: \n{}".format(totals_each_city/counts_each_city))
    # # totals_each_city = totals.sum(axis = 0)
    # print(totals.sum(axis = 0)/12)
    # print(totals.sum(axis = 1))
    print("Average precipitation in each city: \n{}".format(totals.sum(axis = 1)/counts.sum(axis = 1)))




if __name__ == '__main__':
    main()
