import streamlit.components.v1 as components

def my_component():
    component = components.declare_component("component", path="component/frontend")
    return component()