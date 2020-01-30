from app.modules.classes.collection_splitter import SplitCollections


def run():
    splitter = SplitCollections('dti_servers')
    splitter.mass_update()
    print("Complete.")
