import pandas as pd

# q2 Start
tips = pd.read_csv('../yelp_academic_dataset_tip.tsv', sep='\t')
maxTip = tips.groupby(['user_id'])['likes'].sum().max()
print(maxTip)
