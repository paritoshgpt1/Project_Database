import pandas as pd

# q1 Start
q2 = pd.read_csv('yelp_academic_dataset_business.tsv', sep='\t').city.dropna()
cnt = s[s.str.contains('las vegas', case=False)].count()
print(cnt)