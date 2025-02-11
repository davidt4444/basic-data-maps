from postmapper import PostMapper  # Assuming you've saved the previous script in a file named 'post_mapper.py'

def main():
    # Create an instance of PostMapper
    mapper = PostMapper()
    mapper.newer = True

    # Call the method to convert JPost to Post
    mapper.post_to_jpost()

if __name__ == "__main__":
    main()
