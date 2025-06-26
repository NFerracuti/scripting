#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
    source .env
else
    echo "Error: .env file not found"
    exit 1
fi

if [ -z "$EMAIL" ] || [ -z "$JIRA_API" ]; then
    echo "Error: EMAIL or JIRA_API not set in .env file"
    exit 1
fi

echo -e "\n=== Links ==="
echo "JIRA ticket: "
current_branch=$(git branch --show-current)
ticket_number=$(echo $current_branch | grep -o 'ca-[0-9]\+' | grep -o '[0-9]\+')
echo "https://celiapp1.atlassian.net/browse/CA-$ticket_number"
echo "Github PR: "
echo "https://github.com/theceliapp/backend/pulls"
echo "Github file: "
current_working_file=$(git diff --name-only)
echo "https://github.com/theceliapp/backend/blob/main/$current_working_file"
echo -e "\n"

# Prompt for ticket information
echo -e "\n=== Create JIRA Ticket ==="
read -p "Enter ticket summary: " summary
read -p "Enter ticket description: " description
read -p "Enter assignee email (press Enter for self): " assignee_email
read -p "Enter github name: " githubname

# If assignee not provided, default to $EMAIL
if [ -z "$assignee_email" ]; then
    assignee_email=$EMAIL
fi

# Fetch user info from Jira
user_response=$(curl -s \
    -u "$EMAIL:$JIRA_API" \
    -X GET \
    "https://celiapp1.atlassian.net/rest/api/3/users/search?query=$assignee_email" \
    -H "Accept: application/json")

# Extract the correct user's accountId by matching emailAddress
account_id=$(echo "$user_response" | python3 -c '
import sys, json
users = json.load(sys.stdin)
wanted_email = "'$assignee_email'"

for user in users:
    # Some users may not have an emailAddress if they are apps
    if user.get("emailAddress") == wanted_email:
        print(user["accountId"])
        sys.exit(0)
# If no matching user was found, exit with error
sys.exit(1)
')

if [ $? -ne 0 ] || [ -z "$account_id" ]; then
    echo "Error: Could not find a Jira user with email: $assignee_email"
    echo "Please ensure the email matches exactly the one in Jira."
    exit 1
fi

# Create Jira Request JSON
json_data='{
    "fields": {
        "project": {"key": "CA"},
        "summary": "'"$summary"'",
        "description": "'"$description"'",
        "issuetype": {"name": "Task"},
        "assignee": {"accountId": "'"$account_id"'"}
    }
}'

# Create JIRA ticket
response=$(curl -s \
    -u "$EMAIL:$JIRA_API" \
    -X POST \
    -H "Content-Type: application/json" \
    -d "$json_data" \
    "https://celiapp1.atlassian.net/rest/api/2/issue/")

# Extract ticket ID using Python
ticket_id=$(echo "$response" | python3 -c '
import sys, json
try:
    response = json.load(sys.stdin)
    print(response["key"].split("-")[1])
except:
    print("")
')

# Check if ticket creation was successful
if [ -z "$ticket_id" ]; then
    echo "Error creating ticket. Response:"
    echo "$response"
    exit 1
fi

# Print useful information
echo -e "\n=== Ticket Created Successfully ==="
echo "Ticket URL: https://celiapp1.atlassian.net/browse/CA-$ticket_id"
echo "Branch Command: "
branch_name=$(echo "$summary" | tr '[:upper:]' '[:lower:]' | tr ' ' '_')
echo "git checkout -b $githubname/ca-$ticket_id/$branch_name"


## 1. Rename local branch
#git branch -m tania/back-old tania/back-new

## 2. Delete old branch on remote
#git push origin --delete tania/back-old

## 3. Push new branch to remote
#git push origin tania/back-new

## 4. Reset upstream branch
#git push origin -u tania/back-new
