"""
coding challenge 1
create a method to get the index/position of 2 element which equals to required sum
"""
import random, time
from contextlib import contextmanager


@contextmanager
def timeit(app_name: str = "Timeit application"):
    """
    context function to log the run time.
    :param app_name:
    :return:
    """
    start = time.time()
    try:
        print(f"{app_name} --- started")
        yield
    finally:
        print(f"{app_name} --- final processing time in minutes is : {round((time.time() - start) / 60, 5)}")


def get_index(final_sum: int, random_list: list):
    """
    Function to iterate through the list and find the matches
    :param final_sum:
    :param random_list:
    :return:
    """
    index_list = []  # list to store all the index traversed

    # iterating through the list
    for i, x in enumerate(random_list):
        try:
            index = random_list.index(final_sum - x)
            if not (index in index_list and i in index_list):
                index_list.append(index)
                index_list.append(i)
                # printing all the possible combinations
                print(f"Found number {x} and {final_sum - x} at positions {i} and {index} which sums to {final_sum} ")
        except ValueError:
            continue

    if not index_list:
        print(f"did not find 2 integer having sum equal to {sum}")

    return


if __name__ == "__main__":
    with timeit("challenge 1"):
        req_sum = 10  # required sum
        ele_count = 100000  # elements count for testing
        # generating random list
        genlist = [random.randint(-20, 20) for _ in range(ele_count)]
        # Debug printing the list below.
        # print(f"list : {random_list}")

        # get the index of elements having sum equals to required sum
        get_index(req_sum, genlist)

######################################
# the current solution is successfully running for 2.5 minutes for 100000 records.
# the complexity of the solution is O(N) since it has just 1 iterator.
# in case where there are records which can not be loaded to memory then we need to
# 1. utilise the storage for storing the records.
# 2. divide the records in different chunks while storing it.
# 3. we can utilise generators for splitting and chunking the records into multiple files.
# Note: it will load all the records into memory as text.
#       it is recommended to chunk the files at the source of generation.
# 4. once chunked it can be read into while traversing and deleted after the operation using "del" operator
# 5. the method explained is Big data processing framework in nutshell.
# 6. However, the complexity will increase since we will incorporate another loop. I guess O(N^N)
