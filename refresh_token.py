import requests

def refresh_id_token(refresh_token):
    """
    Use the refresh token to get a new ID token.
    """
    token_url = "https://securetoken.googleapis.com/v1/token?key=AIzaSyD39UIdYeMKFi71CJnv7-JNn7z15Sj1-ko"  # Replace with your Firebase API Key

    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token
    }

    response = requests.post(token_url, data=payload)
    if response.status_code == 200:
        data = response.json()
        new_id_token = data.get('id_token')
        new_refresh_token = data.get('refresh_token')  # A new refresh token might be issued
        expires_in = data.get('expires_in')  # Time in seconds before the token expires

        print("New ID Token:", new_id_token)
        print("New Refresh Token (if issued):", new_refresh_token)
        print(f"Expires In: {expires_in} seconds")

        return new_id_token, new_refresh_token
    else:
        print("Error refreshing ID token:", response.json())
        return None, None

# Example usage:
refresh_token = "AMf-vBy4Hi6cmO-Lg95_Zq2HqAsIq7R9voEHg7bb19kb5vdOd_afiFpN5VBzEIp3W0ljS2uYm6axBRqVfG8fTTZ8M4uzfOq6WcFoxTBY9HaBFBUm1-j763HnmrsunvGqjjg08QP8-0pdiXizKYvO6tdH1mIIAizmk5zZdDJTpcL_h50BCnp9bMTWBRaJ5-HXUoFFWJfBfvOWh-Rvk6YMjJJcUeyc2IUq153Jh-BnU6ERtK9cvyhI-mqdwPU0sP4SxHxxJVfbsdTtvGZ3Si3RxQad6rkm41CjzqlHwoEMZ7PCPgvdnUz42oEMiPlXEAVOmuqkA3vrGJlUZu3X3dqZguMtrtgRVnoyrx6tmI9ddaocQPVHV6UKs_vG3wKwGJZia5cdBhvTY4-1FBqr7gJqrbxJSjraVyAuqTKB1JNXDlSAfQeTXOiTG2A"  # Use the refresh token you already have
# client_id = "your_client_id"  # Your OAuth 2.0 Client ID
# client_secret = "your_client_secret"  # Your OAuth 2.0 Client Secret

# Refresh the ID token
new_id_token, new_refresh_token = refresh_id_token(refresh_token)

if new_id_token:
    print("Successfully refreshed ID token!")
