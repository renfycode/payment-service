import asyncio

from payments.consumer.app import create_app


def main() -> None:
    asyncio.run(create_app().run())


if __name__ == "__main__":
    main()
