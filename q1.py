import pandas as pd

# q1 Start
business = pd.read_csv('../yelp_academic_dataset_business.tsv', sep='\t')\
    .city.dropna()
cnt = business[business.str.contains('las vegas', case=False)].count()
print(cnt)
