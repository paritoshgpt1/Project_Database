import pandas as pd
import numpy as np

# q3 Start
tips = pd.read_csv('../yelp_academic_dataset_tip.tsv', sep='\t')
groupedTips = tips.groupby(['user_id'])
filteredTips = groupedTips.filter(lambda s: s['likes'].sum() > 0)
tippedUsers = filteredTips['user_id'].unique()

reviews = pd.read_csv('../yelp_academic_dataset_review.tsv', sep='\t')
reviewUsers = reviews['user_id'].unique()

commonUsers = np.intersect1d(tippedUsers, reviewUsers)
print(len(commonUsers))
