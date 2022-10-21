class Convertor:
    def __init__(self, dic: dict) -> object:
        self.dict = dic

        def recursive_check(obj):
            for key, value in dic.items():
                if isinstance(value, dict):
                    value = Convertor(value)
                setattr(obj, key, value)

        recursive_check(self)
