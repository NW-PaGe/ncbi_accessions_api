import polars as pl
import matplotlib.pyplot as plt

# Read in validation and genbank data
genbank = pl.read_csv('data/genbank_full.csv')
validation = pl.read_csv('data/api_output.csv')

# Create indicator col for use in join indication
genbank = genbank.with_columns(pl.lit(True).alias('indicator'))

# Join genbank to validated dataset and check if accessions match
validation_joined = (
    validation
    .join(genbank, on=['SEQUENCE_GENBANK_STRAIN', 'SEQUENCE_GENBANK_ACCESSION'], how='left')
    .with_columns(pl.col('indicator').fill_null(False))
)

# These records were validated manually
# There exists records in NCBI that have the same strain names with different years as they had different release years
# The strain years and accessions in the GenBank df mismatch the accession and strain years of the validated data
# Mark these records with 'indicator' == True since that represents joined data aka a match with the lookup table
valid_fix = ['USA/WA-UW144/2020', 'USA/WA-S12867/2021', 'USA/WA-UW136/2020', 'USA/WA-S12935/2021',
             'USA/WA-S12899/2021', 'USA/WA-UW137/2020', 'USA/WA-S12934/2021']
validation_joined = validation_joined.with_columns(
    # Change indicator to True
    pl.when(pl.col('SEQUENCE_GENBANK_STRAIN').is_in(valid_fix))
    .then(True)
    .otherwise(pl.col('indicator'))
    .alias('indicator'),
    # Fill specimen collection date with year
    pl.when(pl.col('SEQUENCE_GENBANK_STRAIN').is_in(valid_fix))
    .then(pl.col('SEQUENCE_GENBANK_STRAIN').str.extract(r'/(202[0-9])$'))
    .otherwise(pl.col('SPECIMEN_COLLECTION_DATE'))
    .alias('SPECIMEN_COLLECTION_DATE')
)

# Separate into dfs based on accession and strain matching
correct = validation_joined.filter(pl.col('indicator'))
incorrect = validation_joined.filter(~pl.col('indicator'))

# Join incorrect fields again to see if other accessions are present
incorrect.drop(['SPECIMEN_COLLECTION_DATE', 'indicator']).join(genbank, on='SEQUENCE_GENBANK_STRAIN', how='left')

# Convert scd to Date
def coalesce_dates(col='SPECIMEN_COLLECTION_DATE'):
    return pl.coalesce(
        pl.col(col).str.strptime(pl.Date, "%Y-%m-%d", strict=False),
        pl.col(col).str.strptime(pl.Date, "%Y", strict=False),
        pl.col(col).str.strptime(pl.Date, "%Y-%m", strict=False)
    )
correct = correct.with_columns(coalesce_dates())
incorrect = incorrect.with_columns(coalesce_dates())

# Get the minimum and maximum date range
x_min = pl.concat([correct['SPECIMEN_COLLECTION_DATE'], incorrect['SPECIMEN_COLLECTION_DATE']]).min()
x_max = pl.concat([correct['SPECIMEN_COLLECTION_DATE'], incorrect['SPECIMEN_COLLECTION_DATE']]).max()

def save_plot(series, filename, title_prefix, color):
    bins = pl.date_range(start=x_min, end=x_max, interval='1w', eager=True)  # Create hist bins
    plt.figure(figsize=(10, 6))  # Create plot
    plt.hist(series, bins=bins, color=color)  # Create histogram
    plt.title(title_prefix + 'Result Collection Dates (by week)', color='grey')  # Set title labels
    plt.xlabel('Date', color='grey')  # Set axis labels
    plt.ylabel('Frequency', color='grey')  # Set axis labels
    plt.xlim([x_min, x_max])  # Adjust x-axis limits
    plt.gcf().autofmt_xdate()  # Adjust x-axis label formatting
    ax = plt.gca()  # Get the current Axes object
    ax.tick_params(colors='grey')  # Change tick labels and ticks to grey
    for spine in ax.spines.values(): spine.set_color('grey')  # Change axis lines to grey
    plt.savefig(filename, dpi=300, transparent=True, bbox_inches='tight')  # Save plot

save_plot(correct['SPECIMEN_COLLECTION_DATE'],
          'api_correct.png',
          'Correct ',
          'lightblue')
save_plot(incorrect['SPECIMEN_COLLECTION_DATE'],
          'api_incorrect.png',
          'Incorrect ',
          'orange')
