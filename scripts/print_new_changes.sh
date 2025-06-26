#!/bin/bash

base_path=$(pwd)
declare -a files=(
#    "apis/scan_results_endpoints.py"
#    "apis/reviews_endpoints.py"
#    "apis/productlist_endpoints.py"
#    "apis/urls.py"
#    "apps/shopping/models.py"
#    "apps/shopping/admin.py"
#    "apps/accounts/models.py"
#    "apps/shopping/migrations/0001_initial.py"
#    "apps/shopping/migrations/0003_userreview.py"
#    "integrations/product_details_model.py"
#    "integrations/preferences_model.py"
#    "integrations/openai.py"
#    "integrations/openai_prompt.py"
#    "integrations/perplexity.py"
#    "integrations/perplexity_prompt.py"
    )

{
    for file in "${files[@]}"; do
        file_path="$base_path/$file"
        if [ -f "$file_path" ]; then
            echo "************************************************************************************************"
            echo "******************** FILE: $file"
            cat "$file_path"
            echo
            echo -e "\n\n"
        else
            echo "************************************************************************************************"
            echo "******************** FILE: $file"
            echo "File not found: $file_path"
            echo
            echo -e "\n\n"
        fi
    done
} > scripts/print_entire_context_output1.txt


for output_file in scripts/print_entire_context_output1.txt; do
    printf "%-40s: " "$(basename $output_file)"
    wc -l < "$output_file"
done

echo "Go to:"
echo "scripts/print_entire_context_output1.txt"

#for file in apps/shopping/models.py apps/shopping/admin.py apis/urls.py apis/poll_endpoints.py apis/scan_results_endpoints.py apis/reviews_endpoints.py apis/serializers.py apis/test_scanresults.py apis/test_polls.py apis/test_reviews.py integrations/google_api.py integrations/openai.py integrations/perplexity.py integrations/perplexity_prompt.py integrations/product_details_model.py integrations/preferences_model.py manage.py theceliapp/settings.py theceliapp/settings_local.py theceliapp/settings_prod.py Dockerfile docker-compose.yml cloudbuild.yaml; do
#    printf "%-40s: " "$file"
#    wc -l < "$file"
#done
