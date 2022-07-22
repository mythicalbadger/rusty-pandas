import pandas as pd
import rusty_pandas
from rusty_pandas import DataFrame, Series
import time
import os, sys
def clone_test(pd_df, df):
    print("PANDAS: Copying CSV")
    st = time.time()
    _ = pd_df.copy()
    end = time.time()
    print(f"PANDAS: took {end-st} second")

    print("RP: Copying CSV")
    st = time.time()
    _ = df.copy()
    end = time.time()
    print(f"RP: took {end-st} second")

def sum_test(pd_df, df):
    print("PANDAS: Summing CSV")
    st = time.time()
    _ = pd_df.sum(1, numeric_only=True)
    end = time.time()
    print(f"PANDAS: took {end-st} second")

    print("RP: Summing CSV")
    st = time.time()
    _ = df.sum(0)
    end = time.time()
    print(f"RP: took {end-st} second")

def sum_row_test(pd_df, df):
    print("PANDAS: Summing CSV")
    st = time.time()
    _ = pd_df.sum(0, numeric_only=True)
    end = time.time()
    print(f"PANDAS: took {end-st} second")

    print("RP: Summing CSV")
    st = time.time()
    _ = df.sum(1)
    end = time.time()
    print(f"RP: took {end-st} second")

def mean_col_test(pd_df, df):
    print("PANDAS: Calculating means")
    st = time.time()
    _ = pd_df.mean(1, numeric_only=True)
    end = time.time()
    print(f"PANDAS: took {end-st} second")

    print("RP: Calculating means")
    st = time.time()
    _ = df.mean(0)
    end = time.time()
    print(f"RP: took {end-st} second")

def mean_row_test(pd_df, df):
    print("PANDAS: Calculating means")
    st = time.time()
    _ = pd_df.mean(0, numeric_only=True)
    end = time.time()
    print(f"PANDAS: took {end-st} second")

    print("RP: Calculating means")
    st = time.time()
    _ = df.mean(1)
    end = time.time()
    print(f"RP: took {end-st} second")

def max_col_test(pd_df, df):
    print("PANDAS: Calculating maximum")
    st = time.time()
    _ = pd_df.max(1, numeric_only=True)
    end = time.time()
    print(f"PANDAS: took {end-st} second")

    print("RP: Calculating maximum")
    st = time.time()
    _ = df.max(0)
    end = time.time()
    print(f"RP: took {end-st} second")

def max_row_test(pd_df, df):
    print("PANDAS: Calculating maximum")
    st = time.time()
    _ = pd_df.max(0, numeric_only=True)
    end = time.time()
    print(f"PANDAS: took {end-st} second")

    print("RP: Calculating maximum")
    st = time.time()
    _ = df.max(1)
    end = time.time()
    print(f"RP: took {end-st} second")

filename = "data/2008.csv"

print("PANDAS: Reading CSV")
st = time.time()
pd_df = pd.read_csv(filename)
end = time.time()

print(f"PANDAS: took {end-st} second")

print("RP: Reading CSV")
st = time.time()
df = rusty_pandas.read_csv(filename)
end = time.time()

print(f"RP: took {end-st} second")

#sum_test(pd_df, df)
#sum_row_test(pd_df, df)
#clone_test(pd_df, df)
#mean_col_test(pd_df, df)
#mean_row_test(pd_df, df)
#max_col_test(pd_df, df)
max_row_test(pd_df, df)
