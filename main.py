from config import get_settings
def main():
    settings = get_settings()
    print(settings.model_dump_json(indent=4))

if __name__ == "__main__":
    main()
