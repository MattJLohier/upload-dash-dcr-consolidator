import boto3
import pandas as pd
import io
import json

def lambda_handler(event, context):
    try:
        # Parse input JSON for S3 bucket and object names
        input_bucket = event['input_bucket']
        pivot_object_name = event['pivot_file_key']
        report_object_name = event['report_file_key']
        output_bucket = event['output_bucket']
        output_object_name = event['output_file_key'].replace('.xlsx', '.csv')

        # Create an S3 client
        s3 = boto3.client('s3')

        # Fetch files from S3 and read content into memory
        pivot_content = io.BytesIO(s3.get_object(Bucket=input_bucket, Key=pivot_object_name)['Body'].read())
        report_content = io.BytesIO(s3.get_object(Bucket=input_bucket, Key=report_object_name)['Body'].read())

        # Read the Excel files with pandas

        try:
            # EU Uses This format
            df_pivot = pd.read_excel(pivot_content, sheet_name='Pivot Table Data')
        except ValueError:
            #US uses This format
            try:
                df_pivot = pd.read_excel(pivot_content, sheet_name='Product & Pricing Pivot Data')
            except ValueError:
                print("Neither 'US Pivot' nor 'EU Pivot' sheets were found.")

        try:
            df_report = pd.read_excel(report_content, sheet_name='EU MFP TCO', header=1)
        except ValueError:
            # If 'EU MFP TCO' sheet is not found, try reading 'Product & Pricing Pivot Data' with header=3
            try:
                df_report = pd.read_excel(report_content, sheet_name='Product Details')
            except ValueError:
                # Handle the case where neither sheet is found
                print("Neither 'EU MFP TCO' nor 'Product & Pricing Pivot Data' sheets were found.")

        # Set 'Product' column as index for merging
        if 'UID' in df_pivot.columns and 'UID' in df_report.columns:
            df_pivot.set_index('UID', inplace=True)
            df_report.set_index('UID', inplace=True)
        else:
            missing_columns = []
            if 'UID' not in df_pivot.columns:
                missing_columns.append('Product in pivot data')
            if 'UID' not in df_report.columns:
                missing_columns.append('Product in report data')
            raise ValueError(f"UID column not found in: {', '.join(missing_columns)}")

        # Clean and rename columns
        df_pivot.columns = [col.strip().replace(' ', '_') + '_pivot' for col in df_pivot.columns]
        df_report.columns = [col.strip().replace(' ', '_') + '_report' for col in df_report.columns]

        # Merge data
        merged_df = df_pivot.join(df_report, how='left')
        merged_df.replace(['na', '-'], '', inplace=True)

        # Write result to a CSV file and upload to S3
        output = io.StringIO()
        merged_df.to_csv(output, index=True)
        output.seek(0)

        s3.put_object(Bucket=output_bucket, Key=output_object_name, Body=output.getvalue())

        return {
            'statusCode': 200,
            'body': json.dumps('Merge successful and file uploaded.')
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
