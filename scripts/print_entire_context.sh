#!/bin/bash

cat <<'EOF'
I need help with my django app.

I would like to give you as much context as possible before diving into the questions I have for you:
We are creating an app that helps celiac people find food at the grocery store.
When they are having trouble locating gluten-free food in an aisle, all they have to do is:
  open the app (currently a web app), snap a picture of the shelves containing the groceries, and the app will show them what's gluten free or not!

The app is being developed by a few individuals including myself
- Mobile Frontend:
  - React native + NextJS
  - production app deployed on GCP
  - domain: app.theceliapp.com
  - user data: currently all being stored in Firebase on the frontend
- Backend APIs:
  - Django Rest Framework in Python
  - production deployment: GCP
  - domain: app.theceliapp.com/api
  - Github repo name: backend
  - currently no user data is being stored here on the backend
- Github organization: theceliapp
- Google Cloud Account:
  - project name: theprodkyle
  - domain: theceliapp.ca
  - storage buckets are being used
- Devices: we are all using m1 macbooks

Whenever I ask for help with a feature or with code implementation, please keep in mind the following:
- I need clear and concise instructions
- Instructions should be numbered or formatted into bullet points
- If I mentioned being confused or not understanding a concept, you should remember that I understand better when you use simple analogies
- If we are implementing any code changes, don't forget to mention the names of relevant files.
- Do not give me examples from hypothetical code bases. If you are giving me an example, use the code I have given you. I will be copying and pasting the code you give me, so it needs to work with my existing code. If you need more information, or code from specific files, just ask for it. Don't make up anything unless you already know it will work.
- Do not give me hypothetical code with placeholder methods/functions. If you want to do that, give me the full method. No placeholders. I repeat, no meaningless and empty placeholder code.

EOF

{
    echo "-----------------------------------------------------------------------------------------------------------------------------------------------"
    echo "----------------- TREE STRUCTURE --------------------------------------------------------------------------------------------------------------"
    echo "-----------------------------------------------------------------------------------------------------------------------------------------------"
    tree -a -I '__pycache__|node_modules|logs|__init__.py|static|staticfiles|media|ztesting|zdeprecated|virtualenvceliapp|venv|.git|*.gitignore|.DS_Store|.idea|*venv*|*env*|.github|.ipynb_checkpoints|private|image_processor|scraped_output|tests|test_images|scripts' .

    echo "-----------------------------------------------------------------------------------------------------------------------------------------------"
    echo "----------------- CLASSES AND FUNCTIONS -------------------------------------------------------------------------------------------------------"
    echo "-----------------------------------------------------------------------------------------------------------------------------------------------"
    grep -r -E '^(class|def) ' --include='*.py' \
    --exclude-dir='*venv*' \
    --exclude-dir='*env*' \
    --exclude-dir='.github' \
    --exclude-dir='*ipynb_checkpoints*' \
    --exclude-dir='private' \
    --exclude-dir='image_processor' \
    --exclude-dir='scraped_output' \
    --exclude-dir='tests' \
    --exclude-dir='scripts/.ipynb_checkpoints' \
    --exclude-dir='scripts/private' \
    --exclude-dir='scripts/test_images' \
    --exclude-dir='scripts/image_processor' \
    --exclude-dir='scripts/scraped_output' \
    --exclude='views_deprecated.py' \
    $(grep -v '^#' .gitignore | grep -v '^$' | sed 's|$|/|; s|^|--exclude-dir=|') \
    .

echo "-----------------------------------------------------------------------------------------------------------------------------------------------"
echo "----------------- DJANGO CONFIGURATION FILES --------------------------------------------------------------------------------------------------------------"
echo "-----------------------------------------------------------------------------------------------------------------------------------------------"
    for file in "manage.py" "theceliapp/settings.py" "theceliapp/settings_local.py" "theceliapp/settings_prod.py"; do
        echo "******************** FILE: $file ********************"
        cat "$file"
        echo
    done


echo "-----------------------------------------------------------------------------------------------------------------------------------------------"
echo "----------------- DEPLOYMENT CONFIGURATION --------------------------------------------------------------------------------------------------------------"
echo "-----------------------------------------------------------------------------------------------------------------------------------------------"
    for file in "Dockerfile"  "docker-compose.yml" "cloudbuild.yaml" "README.md"; do
        echo "******************** FILE: $file ********************"
        cat "$file"
        echo
    done
} > scripts/print_entire_context_output1.txt

# Print output into 3 parts:
base_path=$(pwd)
declare -a files=(
    # print_entire_context_output2.txt
    "apps/shopping/admin.py"
    "apis/serializers.py"
    "apis/urls.py"
    "apis/reviews_endpoints.py"
    "integrations/openai.py"

    # print_entire_context_output3.txt
    "apps/shopping/models/product.py"
    "integrations/perplexity.py"
    "apis/productlist_endpoints.py"

    # print_entire_context_output4.txt
    "apis/scan_results_endpoints.py"
    "integrations/perplexity_prompt.py"
    "apps/shopping/models/review.py"
    "integrations/google_api.py"
    )

# Set split sizes
start=0
middle=5
last=8

# Output 2: First third of files
{
    echo "# First set of files"
    for ((i=start; i<middle; i++)); do
        file_path="$base_path/${files[$i]}"
        echo "************************************************************************************************"
        echo "******************** FILE $((i+1)) NAME: $file_path"
        cat "$file_path"
        echo
    done
} > scripts/print_entire_context_output2.txt

# Output 3: Second third of files
{
    echo "# Second set of files"
    for ((i=middle; i<last; i++)); do
        file_path="$base_path/${files[$i]}"
        echo "************************************************************************************************"
        echo "******************** FILE $((i+1)) NAME: $file_path"
        cat "$file_path"
        echo
    done
} > scripts/print_entire_context_output3.txt

# Output 4: Final third of files
{
    echo "# Final set of files"
    for ((i=last; i<${#files[@]}; i++)); do
        file_path="$base_path/${files[$i]}"
        echo "************************************************************************************************"
        echo "******************** FILE $((i+1)) NAME: $file_path"
        cat "$file_path"
        echo
    done
} > scripts/print_entire_context_output4.txt



cat <<'EOF'
------------------------------------------------------------------------------------
Then I want you to summarize the following for me:
1) What is the purpose of this app?
2) How does the backend work - what are the most important things about it? What are the requests and responses?
3) How does the web repo work - what are the most important things about it? What are the requests and responses?
4) How do backend and web connect together? What do you think is missing?
5) What are the differences between the GCP deployments for each?
6) what are the most important things to know about the GCP deployments?
7) What was the beginning and last line that you read in each of the 4 files?
Do you have any questions?
EOF

echo "------------------------------------------------------------------------------------"
for output_file in scripts/print_entire_context_output{1,2,3,4}.txt; do
    printf "%-40s: " "$(basename $output_file)"
    wc -l < "$output_file"
done

echo "------------------------------------------------------------------------------------"
for file in "${files[@]}"; do
    printf "%-40s: " "$file"
    wc -l < "$file"
done
