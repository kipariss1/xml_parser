from dataclasses import dataclass


@dataclass
class BaseStructureClass:
    lxml_element = None
    parent = None

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def get_child_attr_by_matching_property(
            self,
            wanted_property: str,
            to: str,
            child: str
    ):
        attrs = self[child + 'S']
        if not attrs:
            return None
        res = []
        for el in attrs:
            if getattr(el, wanted_property) == to:
                res.append(el)
        return res


class StructureElementsFactory:

    @classmethod
    def add_attributes(cls, upd_cls, attr: dict):
        for key, val in attr.items():
            upd_cls[key] = val

    @classmethod
    def derive_new_class(cls, class_name: str, attributes: dict, base_class=BaseStructureClass):
        new_class = type(class_name, (base_class,), attributes)
        return new_class

    @classmethod
    def derive_and_init(cls, class_name: str, attributes: dict, lxml_element, parent, base_class=BaseStructureClass):
        new_class = type(class_name, (base_class,), {key.lower(): val for key, val in attributes.items()})
        new_class()
        new_class.lxml_element = lxml_element
        new_class.parent = parent
        return new_class
