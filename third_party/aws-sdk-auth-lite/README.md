# aws-sdk-auth-lite

The following is an extraction of the important auth interfaces from the AWS SDK.
These interfaces are only those needed to establish credentials with AWS, and include nothing else
from the AWS SDK.

Some very small changes have been made to remove the dependencies on Joda-time, jackson and other similar dependencies.
A few small changes have been made to use guava utilities over those provided by the original AWS SDK.
