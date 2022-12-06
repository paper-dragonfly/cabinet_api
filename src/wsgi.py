from src.api import create_app

ENV = 'testing'
app = create_app(ENV)

if __name__ == '__main__':
    app.run 