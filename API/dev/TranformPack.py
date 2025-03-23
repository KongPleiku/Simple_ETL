import pandas as pd

class Base_Tranform:
    def __init__(self, df: pd.DataFrame):
        self.df = df