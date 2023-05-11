def get_user_credentials() -> tuple:

    login = input("'PyParser version='0.0.1'\n\n"
                  "To continue, please enter login: ").strip()
    password = input('Enter your password: ').strip()

    return login, password

