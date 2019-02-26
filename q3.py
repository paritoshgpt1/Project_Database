import pandas as pd

# q3 Start
tips = pd.read_csv('../yelp_academic_dataset_tip.tsv', sep='\t')
reviews = pd.read_csv('../yelp_academic_dataset_review.tsv', sep='\t')

merged = pd.merge(tips, reviews, how='inner', on='user_id')
grouped = merged.groupby(['user_id'])
print(len(grouped))
