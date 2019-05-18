import numpy as np




def main():
    data = np.load('monthdata.npz')
    totals = data['totals']
    counts = data['counts']
    result1 = totals.sum(axis=1)
    # print(totals)
    print("{} row has the lowest value".format(np.argmin(result1)))
    precipitation = totals.sum(axis=0)
    observations = counts.sum(axis=0)
    print("the average precipitation for that months\n: {}".format(precipitation/observations))

    # Do the same for the cities: give the average daily precipitation for each city by printing the array.
    precipitation2 = totals.sum(axis=1)
    observations2 = counts.sum(axis=1)
    print("the average precipitation for each city: \n {}".format(precipitation2/observations2))

    frame = counts.reshape(36, 3)
    # print(frame)
    result3 = frame.sum(axis=1)
    result3 = result3.reshape(9, 4)
    print("the total precipitation for each quarter in each city: \n {}".format(result3))

if __name__ == '__main__':
    main()
