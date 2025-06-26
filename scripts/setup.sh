#!/bin/bash
# RUN THIS SCRIPT TO AUTOMATICALLY SET UP THIS LOCAL ENVIRONMENT

echo "~~~ Update pip ~~~"
python3 -m pip install -U pip


echo "~~~ Install dependencies ~~~"
python3 -m pip install -r requirements.txt


echo "~~~ Check if jq is installed, if not, install it ~~~"
if ! command -v jq &> /dev/null
then
    echo "~~~ jq could not be found. Installing jq... ~~~"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install jq
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        sudo apt-get install -y jq
    else
        echo "Please install jq manually from https://stedolan.github.io/jq/download/"
        exit 1
    fi
else
    echo "~~~ jq is already installed ~~~"
fi

echo "~~~ Create & Activate a virtual environment ~~~"
echo "skipping - virtual env not necessary"
#python3 -m venv myenv
#source myenv/bin/activate


echo "~~~ Make migrations & collect staticfiles ~~~"
python3 manage.py makemigrations --noinput
python3 manage.py migrate --noinput
python3 manage.py collectstatic --noinput --clear
echo "Build process completed!"


echo "~~~ Run your Django application ~~~"
python3 manage.py runserver


echo "~~~ Update file permission for all script files~~~ "
find ./scripts -type f -iname "*.sh" -exec chmod +x {} \;


echo "~~~ Create local .env file and populate it ~~~"
touch .env
echo "DEVELOPMENT_MODE=True" > .env
echo "WHICH_ENVIRONMENT=local_machine_root" >> .env
echo "DJANGO_SETTINGS_MODULE='theceliapp.settings_prod'" >> .env


echo "~~~ Enter credentials ~~~"
echo "skipping: don't want to overwrite anything accidentally"
#echo "PERPLEXITY_API_KEY=pplx-ask_me_for_this" >> .env
#echo "CHATGPT_API_KEY=ask_me_for_this" >> .env
#echo "APPLICATION_DEFAULT_CREDENTIALS='{ask_me_for_this}'" >> .env
#echo "GOOGLE_APPLICATION_CREDENTIALS_NEW='{ask_me_for_this}'" >> .env
#echo "GOOGLE_DB_PASSWORD=ask_me_for_this" >> .env


# Build and start the containers
#docker-compose up -d --build
#docker-compose run back python manage.py migrate
#docker-compose run back python manage.py createsuperuser
#echo "Setup complete! The backend is now running at http://localhost:8000"
