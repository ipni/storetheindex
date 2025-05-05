# Place any environment variables you want to available to your docker container 
# here. If you want to use different values based on terraform vars or other 
# values in env.terraform.tpl, this script is processed by ESH 
# (https://github.com/jirutka/esh) and copied to env.production.local before it 
# is used, so you can use that functionality to interpolate variables and 
# generally do conditional rendering based on bash scripting.