"""Data Cleaninig Module"""

import datetime
import pandas as pd
import dateutil.parser as parser

class DataCleaning:
    """Methods for cleaning Data"""
    # import pandas as pd # remove

    def __init__(self, dataframe:pd.DataFrame) -> None:
        self.dataframe = dataframe

    def clean_user_data(dataframe:pd.DataFrame):
        """Cleaning NULL values, erroneous dates, typos and wrong info """
        # import pandas as pd # remove

        def nullstrcheck(dataframe:pd.DataFrame):
            """Checks if any field has a 'NULL' entry"""
            numrows = []
            for column in dataframe.columns:
                nullstrcheck = dataframe[str(column)].apply(lambda x : "NULL" in x)
                rowremoval = dataframe.loc[nullstrcheck].index.values
                numrows.append(len(rowremoval))
                dataframe = dataframe.drop(index=rowremoval) # to protect the integrity of the orignal data we have opted to not make the change in place (inplace = True)
            return dataframe

        def atchecker(dataframe:pd.DataFrame):
            """Checks if the email contains '@' """
            numrows = []
            atsymbolcheck =  dataframe["email_address"].apply(lambda x : "@" not in x)
            rowremoval = dataframe.loc[atsymbolcheck].index.values
            numrows.append(len(rowremoval))
            dataframe = dataframe.drop(index=rowremoval)
            return dataframe

        def dotchecker(dataframe:pd.DataFrame):
            """Checks if the email contains '.' """
            numrows = []
            dotsymbolcheck =  dataframe["email_address"].apply(lambda x : "." not in x)
            rowremoval = dataframe.loc[dotsymbolcheck].index.values
            numrows.append(len(rowremoval))
            dataframe = dataframe.drop(index=rowremoval)
            return dataframe

        def phonenumberchecker(dataframe:pd.DataFrame):
            """Checks if the phone number starts with a letter """
            numrows = []
            numbercheck =  dataframe["phone_number"].apply(lambda x : str(x[1]).isalpha())
            rowremoval = dataframe.loc[numbercheck].index.values
            numrows.append(len(rowremoval))
            dataframe = dataframe.drop(index=rowremoval)
            return dataframe

        null_cleaned = nullstrcheck(dataframe)
        email_no_at_cleaned = atchecker(null_cleaned)
        email_no_dot_cleaned = dotchecker(email_no_at_cleaned)
        phone_no_clean = phonenumberchecker(email_no_dot_cleaned)
        cleaned_df = phone_no_clean
        return cleaned_df


    def clean_card_data(dataframe:pd.DataFrame):
        """Clean card data"""

        def na_drop(dataframe:pd.DataFrame):
            """Drop na values from DataFrame"""
            dataframe = dataframe.dropna()
            return dataframe

        def card_number(dataframe:pd.DataFrame):
            dataframe = dataframe[dataframe["card_number"].apply(lambda x :x.isnumeric())]
            return dataframe

        def date_check(dataframe:pd.DataFrame):
            dataframe["date"] = 0
            for x in range(len(dataframe)):
                try:
                    dataframe["date"][x] = datetime.datetime.isoformat(parser.parse(dataframe["date_payment_confirmed"][x]))
                except:
                    dataframe["date"][x] = pd.NA # If there is a problem converting to a date, deem the element as Na

            dataframe = dataframe.dropna() # drop rows with bad dates
            return dataframe

        na_cleaned = na_drop(dataframe)
        card_number_cleaned = card_number(na_cleaned)
        date_check_cleaned = date_check(card_number_cleaned)
        cleaned_card_data = date_check_cleaned

        return cleaned_card_data

    def called_clean_store_data(dataframe:pd.DataFrame):
        """Cleans store data obtained from API call"""
        # import pandas as pd
        def nacheck(dataframe:pd.DataFrame = dataframe):
            """Checks if any field is an NA entry"""
            dataframe.drop(columns="lat",inplace=True) # useless duplicate column
            numrows = []
            for column in dataframe.columns:
                nacheck = dataframe[str(column)].apply(lambda x : x  == pd.NA)
                try:
                    rowremoval = dataframe.loc[nacheck].index.values
                    numrows.append(len(rowremoval))
                    dataframe.drop(index=rowremoval,inplace= True)
                except:
                    pass
            return dataframe

        nachecked =  nacheck()

        def nullstrcheck(dataframe = nachecked):
            numrows = []
            # for string in dataframe["address"]:
            nullstrcheck = dataframe["address"].apply(lambda x : "NULL" in x)
            rowremoval = dataframe.loc[nullstrcheck].index.values
            numrows.append(len(rowremoval))
            dataframe.drop(index=rowremoval,inplace= True)
            return dataframe

        nullstrchecked  = nullstrcheck()

        def intcheck(dataframe=nullstrchecked):
            dataframe["longitude"] = dataframe["longitude"].apply(lambda x : pd.to_numeric(x,errors="coerce")) # any value that cannot be converted will be nullified
            dataframe.dropna(inplace=True)
            return dataframe
        return intcheck()

    def convert_product_weights(dataframe:pd.DataFrame):
        """Converts raw weights of products to kg"""
        product_df = dataframe.copy()
        product_df.dropna(inplace=True)

        def unit_check(weight):
            # mutlipack check
            if "x" in weight:
                unitweight_quantity = weight.split()
                if len(unitweight_quantity) == 4: # ....if in the form 3 x 20 g
                    net_weight = float(unitweight_quantity[0]) * float(unitweight_quantity[2])
                    weight = str(net_weight) + weight[-1] # Put original unit back
                elif len(unitweight_quantity) == 3: # ....if in the form 3 x 20g
                    net_weight = float(unitweight_quantity[0]) * float(unitweight_quantity[2][:-1]) #separate g from last num (20g becomes 20)
                    weight = str(net_weight) + weight[-1]

            # unit check and conversion to kg
            if weight[-2:] == "kg":
                weight = float(weight[:-2])

            elif weight[-2:] == "ml":
                weight = float(weight[:-2])*1000

            elif weight[-1:] == "g":
                weight = float(weight[:-1])*1000

            return weight

        product_df["weight"] = product_df["weight"].apply(lambda x : unit_check(x)) # run unit_check function on weight column and update
        return product_df

    def clean_products_data(dataframe=convert_product_weights):
        """Clean additional errorneous values"""
        #cleaning was done in convert_product_weights(drop na)
        return dataframe

    def clean_orders_data(dataframe :pd.DataFrame):
        """"Cleans order data"""
        orders_df = dataframe.copy()
        orders_df.drop(columns=["first_name","last_name", "1"], inplace=True)
        orders_df.dropna(inplace=True)
        return orders_df

    def clean_events_data(dataframe :pd.DataFrame):
        """Cleans events data"""
        events_df = dataframe.copy()
        events_df["date_uuid_check"] = events_df["date_uuid"].apply(lambda x : None if x == "NULL" else x ) # check if any null values in date_uuid
        events_df.dropna(inplace=True)
        events_df.drop(columns=["date_uuid_check"],inplace=True)
        return events_df


