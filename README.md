## ExportFeed Template Parser

#### You mush edit copy config.example.json to config.json and change the content inside it
```
{
  "template_csv_file_path": "/path/to/your/template_file.csv",
  "template_directory_path": "/path/to/your/template/directory",
  "output_directory_path": "/path/to/your/output/directory",
  "flat_file_placeholder": "Flat.File.{}-{}.csv",
  "template_table_name": "templates_updated",
  "template_values_table_name": "template_values_updated",
  "create_csv": 1,
  "filter_country": []
}
```
#### Explaination of each key
1. template_csv_file_path
   - Your template file that contains category details, country code, and template id
   - This is the base file to find which file to process
2. template_directory_path
   - Containing your .csv files for each of the category mentioned in above file
   - Every row in the file template_csv_file_path should have .csv file in this directory under country code directory
   - There should be exactly 3 files, in each sub folder prepended by DataDefinations, Template and ValidValues
3. output_directory_path
   - Output folder after data is generated. The output file with extension .sql and .csv files will be generated here
4. flat_file_placeholder
   - This is how your .csv files (3 files) template_directory_path is named.
   - This is your file format Flat.File.{}-{}.csv, The first {} denotes Category name and second {} denotes 3 different files (DataDefinations, Template, ValidValues)
5. template_table_name
   - This is output table name for sql file and csv file generated from *Template.csv file above
6. template_values_table_name
   - This is output table name for sql file and csv file generated from *DataDefinations.csv and *ValidValues.csv files above
7. create_csv
   - 0 or 1, whether or not csv file should be generated or not respectively
8. filter_country
   - Leave empty for processing all countries or add to include only them