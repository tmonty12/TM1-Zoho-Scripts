# TM1-Zoho-Scripts
These scripts are apart of a TM1 model that is used for tracking Pipeline projects.

The unique_alias script takes in a flat file, compares columns that have been passed in, finds the duplicates and creates a new column to increment the duplicates for creating
aliases.

The zoho_data_pull script connects with the Zoho api and writes out records to a flat file based off the columns passed in as arguments.

The zoho_api_update script compares an Update element to a Current element in a TM1 cube dimension, finds the updates that have been made to the records, sends an http request
to the Zoho api to update the records and updates Current element to match the Update element.
