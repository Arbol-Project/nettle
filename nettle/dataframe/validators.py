import pandas as pd
from nettle.errors.custom_errors import DataframeInvalidException

class DataframeValidator:
    @staticmethod
    def df_columns_not_in_dict(
            dataframe: pd.DataFrame,
            data_dict: dict
    ):
        df_properties = list(dataframe.columns)
        data_dict_properties = [v["column name"] for v in data_dict.values()]
        return [x for x in df_properties if x not in data_dict_properties]

    @staticmethod
    def not_strings_df_columns(
            dataframe: pd.DataFrame
    ):
        return dataframe.dtypes[dataframe.dtypes != 'object']

    @staticmethod
    def validate(
            dataframe: pd.DataFrame,
            data_dict: dict
    ):
        # Check if all the elements are in data dict
        diff_properties = DataframeValidator.df_columns_not_in_dict(dataframe, data_dict)
        if diff_properties:
            raise DataframeInvalidException(
                f'Columns in processed dataframe not in data dictionary: {diff_properties}'
            )

        # Check if all columns are strings
        not_strings_df_columns = DataframeValidator.not_strings_df_columns(dataframe)
        if not not_strings_df_columns.empty:
            raise DataframeInvalidException(
                f'All dataframes columns needs to be a string. '
                f'\nThose columns are not strings: \n{not_strings_df_columns}'
            )

