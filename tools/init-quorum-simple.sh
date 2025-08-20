#!/bin/bash
# Simple S3 Quorum Storage Initialization Script
# This script modifies the configuration to work around the object initialization issue

set -e

CONFIG_FILE="${1:-config/kraft-s3-quorum-development.properties}"
BACKUP_FILE="${CONFIG_FILE}.backup.$(date +%Y%m%d_%H%M%S)"

echo "🔧 AutoMQ S3 Quorum Storage Simple Initialization"
echo "=================================================="

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "❌ Error: Configuration file not found: $CONFIG_FILE"
    exit 1
fi

echo "📁 Configuration file: $CONFIG_FILE"
echo "💾 Creating backup: $BACKUP_FILE"
cp "$CONFIG_FILE" "$BACKUP_FILE"

echo "⚙️  Applying initialization workarounds..."

# Create a temporary configuration that starts with basic S3 Stream
# and then enables Quorum features gradually

# 1. Enable mock mode initially to avoid object dependencies
sed -i.tmp 's/^s3\.mock\.enable=false/s3.mock.enable=true/' "$CONFIG_FILE"

# 2. Comment out problematic Quorum configurations temporarily
sed -i.tmp 's/^automq\.s3\.quorum\.enabled=true/#automq.s3.quorum.enabled=true/' "$CONFIG_FILE"

# 3. Add a flag to indicate this is an initialized configuration
echo "" >> "$CONFIG_FILE"
echo "# Auto-initialized by init-quorum-simple.sh on $(date)" >> "$CONFIG_FILE"
echo "# This configuration has been modified to work around S3 object initialization issues" >> "$CONFIG_FILE"
echo "automq.s3.init.workaround.applied=true" >> "$CONFIG_FILE"

# Remove temporary files
rm -f "${CONFIG_FILE}.tmp"

echo "✅ Configuration modified successfully!"
echo ""
echo "📝 Changes applied:"
echo "   - Enabled mock mode (s3.mock.enable=true)"
echo "   - Temporarily disabled Quorum Storage"
echo "   - Added initialization flag"
echo ""
echo "🚀 Next steps:"
echo "1. Start Kafka with the modified configuration:"
echo "   bin/kafka-storage.sh format -t \$(bin/kafka-storage.sh random-uuid) -c $CONFIG_FILE"
echo "   bin/kafka-server-start.sh $CONFIG_FILE"
echo ""
echo "2. Once the server starts successfully, you can restore the original configuration:"
echo "   cp $BACKUP_FILE $CONFIG_FILE"
echo ""
echo "3. Restart Kafka with the restored configuration to enable full Quorum Storage"
echo ""
echo "💡 This workaround allows the system to create necessary metadata objects"
echo "   during the initial startup, which prevents the 'object not exist' errors."