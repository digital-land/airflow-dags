# EMR Multi-Dataset Processing

This directory contains an enhanced version of the EMR Serverless DAG that can process multiple datasets in parallel, with secure configuration management using AWS Secrets Manager or Parameter Store.

## 🚀 Key Features

- **Parallel Processing**: Each dataset spawns its own EMR Serverless job for maximum efficiency
- **Secure Configuration**: Dataset lists stored in AWS Secrets Manager or Parameter Store
- **Dynamic Task Mapping**: Airflow automatically creates tasks for each dataset
- **Robust Error Handling**: Individual dataset failures don't affect others
- **Comprehensive Monitoring**: Per-dataset logging and status tracking
- **Cost Optimization**: Automatic job cancellation on timeout

## 📁 Files

- `emr_sl_testing_multi.py`: Main DAG with multi-dataset support
- `scripts/setup_datasets_config.py`: Setup script for configuring dataset lists
- `aws_secrets_manager.py`: Existing secrets management utilities

## 🔧 Setup Instructions

### 1. Configure Dataset List

Run the setup script to store your dataset list securely:

```bash
cd /path/to/airflow-dags
python scripts/setup_datasets_config.py
```

The script will:
- Prompt you to enter datasets (comma-separated)
- Let you choose between Secrets Manager or Parameter Store
- Verify the configuration works
- Show required IAM permissions

### 2. Storage Options

#### Option A: AWS Secrets Manager (Recommended)
- More secure for sensitive data
- Secret name: `development/emr_datasets_list`
- Format: JSON array `["dataset1", "dataset2", ...]`

#### Option B: AWS Parameter Store
- Good for configuration data
- Parameter name: `/emr/datasets-list`
- Type: SecureString (encrypted)
- Format: JSON array `["dataset1", "dataset2", ...]`

#### Option C: Environment Variable (Testing)
- Set `EMR_DATASETS_LIST` environment variable
- Format: JSON array as string

### 3. IAM Permissions

Ensure your Airflow execution role has these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:eu-west-2:*:secret:development/emr_datasets_list*"
    },
    {
      "Effect": "Allow", 
      "Action": [
        "ssm:GetParameter"
      ],
      "Resource": "arn:aws:ssm:eu-west-2:*:parameter/emr/datasets-list"
    },
    {
      "Effect": "Allow",
      "Action": [
        "emr-serverless:StartJobRun",
        "emr-serverless:GetJobRun",
        "emr-serverless:CancelJobRun"
      ],
      "Resource": "*"
    }
  ]
}
```

## 🏃‍♂️ Usage

### Running the DAG

1. **Deploy the DAG**: Copy `emr_sl_testing_multi.py` to your Airflow DAGs folder
2. **Trigger manually**: The DAG is set to manual trigger only
3. **Monitor progress**: Each dataset will have its own task in the Airflow UI

### Task Flow

```
get_datasets() 
    ↓
submit_emr_job_for_dataset_task (parallel for each dataset)
    ↓ 
wait_for_emr_job_completion_task (parallel monitoring)
    ↓
summarize_results()
```

### Example Dataset Configuration

```json
[
  "ancient-woodland",
  "conservation-area", 
  "listed-building",
  "tree-preservation-order",
  "article-4-direction"
]
```

## 📊 Monitoring

### Airflow UI
- Each dataset gets its own submit and wait tasks
- Task names include dataset names for easy identification
- Failed datasets don't block successful ones

### Logs
- Per-dataset logging with clear prefixes
- Job submission details and progress updates
- Resource utilization reporting on completion

### AWS Console
- Each dataset creates a separate EMR Serverless job
- Jobs named with dataset and timestamp: `{dataset}-job-{timestamp}`
- Individual cost tracking per dataset

## 🔒 Security Considerations

### Why Multiple Storage Options?

1. **AWS Secrets Manager**: Best for sensitive datasets or when you need rotation
2. **AWS Parameter Store**: Good for configuration data, more cost-effective for simple lists
3. **Environment Variables**: Only for development/testing

### Best Practices

- Use Secrets Manager for production environments
- Store sensitive dataset configurations separately
- Regularly rotate secrets if they contain sensitive information
- Use least-privilege IAM policies

## 🚨 Error Handling

### Individual Dataset Failures
- Failed datasets don't affect others
- Each failure is logged with dataset context
- Summary report shows successful vs failed datasets

### Timeout Protection
- 15-minute timeout per dataset job
- Automatic job cancellation on timeout
- Prevents runaway costs

### Fallback Behavior
- Falls back to single dataset if configuration fails
- Environment variable override for testing
- Graceful degradation

## 💡 Benefits Over Single Dataset Approach

### Performance
- **Parallel Execution**: All datasets process simultaneously
- **EMR Serverless Scaling**: Each job can scale independently
- **No Sequential Bottlenecks**: Faster datasets don't wait for slower ones

### Reliability
- **Fault Isolation**: One dataset failure doesn't affect others
- **Independent Monitoring**: Per-dataset status tracking
- **Retry Capabilities**: Can retry individual failed datasets

### Cost Efficiency
- **Resource Optimization**: Each job uses optimal resources for its dataset
- **Timeout Protection**: Prevents runaway costs
- **Usage Tracking**: Clear cost attribution per dataset

### Operational Benefits
- **Clear Monitoring**: Easy to see which datasets succeeded/failed
- **Flexible Configuration**: Easy to add/remove datasets
- **Secure Management**: Dataset lists not hardcoded in repository

## 🔄 Migration from Single Dataset

To migrate from the existing single dataset DAG:

1. **Test First**: Run with a small subset of datasets
2. **Verify Permissions**: Ensure IAM roles have required permissions
3. **Monitor Resources**: Check EMR Serverless capacity limits
4. **Update Scheduling**: Consider resource usage when scheduling

## 📈 Scaling Considerations

### EMR Serverless Limits
- Check AWS service limits for concurrent jobs
- Monitor EMR application capacity
- Consider staggered job submission for very large dataset lists

### Airflow Resources
- Dynamic task mapping creates many tasks
- Monitor Airflow worker capacity
- Consider task concurrency limits

## 🛠️ Troubleshooting

### Common Issues

1. **"No datasets found"**: Check configuration in Secrets Manager/Parameter Store
2. **Permission denied**: Verify IAM permissions for secrets access
3. **EMR job failures**: Check EMR logs in S3 monitoring configuration
4. **Timeout issues**: Consider increasing timeout for large datasets

### Debug Mode

Set environment variable for testing:
```bash
export EMR_DATASETS_LIST='["test-dataset"]'
```

This bypasses Secrets Manager/Parameter Store for debugging.
