# Data-linkage-and-deduplication-using-Splink

In this project use the 'Splink' record linkage and deduplication library in python that uses probabilistic record linkage to identify and link and/or merge duplicate records from a dataset that lack unique identifiers.

We made Splink compare person records using below fields:
Names (given_name_1, given_name_2, given_name_3, surname) – accounts for spelling variations, typos, and missing middle names.
Date of Birth (date_of_birth) – considers the variations in how this information may incorrectly be entered.
Sex (sex)
Address Information (suburb_code, postcode) – people live in different places over time.
Contact Number (contact_number) – a strong identifier when available.

It outputs a match_weight (Log-likelihood ratio - 0 to 15+) for each pair of records, along with a match_probability (0 to 1).
We determined the match_weight was a more suitable indicator of whether two records are likely to be duplicates, as it's a measure of evidence strength.
Each comparison field (above) contributes a partial match weight, and the total match weight is the sum of all these partial weights across all fields.
A threshold for match_weight was determined by manually inspecting pairs to identify false positives/negatives and balancing precision (avoiding false matches) and recall (finding true duplicates).

How we determine which record to retain when duplicates are found:
After Splink compares records and assigns match_weight, it groups records into clusters where each cluster represents a unique person.
This function clusters records together based on our similarity threshold (say >= 22). If the match score between two records exceeds the threshold, they are linked into the same cluster.
The master record is chosen in a cluster as the earliest recorded entry based on the 'id' identifier.
For example: if person record with id = 100 and 101 are grouped in the same cluster the id = 100 will be selected as the master record for this cluster.

On combining partial information from duplicate records if needed:
To ensure the selected master person record per cluster, has the most complete information (i.e. least missing values) we can consolidate information from across all the records found in the cluster.
Where the master record is missing information, we can populate it with information from other records in the order of most recent updated record.

Validate accuracy of person merging models:
In short: Come up with a control set (known edge cases that shouldn’t be merged) to check what the linking model (and each iteration) does.
Synthetic person data is generated and loaded into a table. This dataset has known cases that shouldn't be merged and records that should.
The same person linking model used to de-duplicate the person records is applied to this dataset.
A flag column `should_not_merge` indicates whether records are not expected to be merged, helping assess whether the model correctly identifies true matches and avoids false positives.
Validation is carried out using the appropriate repo, which checks and produces a report on the model’s performance.

References:

https://github.com/moj-analytical-services/splink

  https://github.com/moj-analytical-services/splink/blob/master/docs/demos/tutorials/01_Prerequisites.ipynb
  
  https://github.com/moj-analytical-services/splink/blob/master/docs/topic_guides/comparisons/out_of_the_box_comparisons.ipynb
  
  https://github.com/moj-analytical-services/splink/blob/24667bb17df13a01a55d881563ed27a197cdfc87/docs/topic_guides/data_preparation/feature_engineering.md#feature-engineering-for-data-linkage
  
  https://moj-analytical-services.github.io/splink/demos/tutorials/01_Prerequisites.html
  
  https://moj-analytical-services.github.io/splink/api_docs/comparison_library.html
  
  https://moj-analytical-services.github.io/splink/topic_guides/blocking/model_training.html
  
https://nicd.org.uk/knowledge-hub/an-end-to-end-guide-to-overcoming-unique-identifier-challenges-with-splink

https://www.youtube.com/watch?v=msz3T741KQI
