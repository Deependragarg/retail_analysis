import datetime

def greet_user():
    name = input("What's your name? ")
    now = datetime.datetime.now()
    print(f"Hello, {name}!")
    print(f"Today is {now.strftime('%A, %B %d, %Y')}")
    print(f"The current time is {now.strftime('%I:%M %p')}")

if __name__ == "__main__":
    greet_user()
