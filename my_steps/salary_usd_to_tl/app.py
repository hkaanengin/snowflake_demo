import sys

def main(salary: float) -> float:
    return (round(salary * 28.82, 2))


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    print(main(float(sys.argv[1])))