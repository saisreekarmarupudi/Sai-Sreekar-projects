# Databricks notebook source
# MAGIC %pip uninstall numpy -y
# MAGIC %pip install "numpy>=1.16.5,<1.23.0"
# MAGIC %pip install tensorflow
# MAGIC %pip install opencv-python-headless
# MAGIC

# COMMAND ----------

import os
import tarfile
import urllib.request
import cv2
import numpy as np
# Model downloading logic remains the same...
from sklearn.cluster import KMeans
import tensorflow as tf
print("Loading EfficientNetB0 model...")
model = tf.keras.applications.EfficientNetB0(weights='imagenet')
print("Model loaded.")


# COMMAND ----------

from tensorflow.keras.applications.efficientnet import preprocess_input, decode_predictions

def run_detection(image):
    # Resize and preprocess the image for the model
    resized_image = tf.image.resize(image, (224, 224))
    preprocessed_image = preprocess_input(resized_image)
    input_tensor = tf.convert_to_tensor([preprocessed_image], dtype=tf.float32)

    # Run the model and return the predictions
    predictions = model(input_tensor)
    return predictions

def find_dominant_color(image, k=1):
    # Convert the image to RGB color space
    image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
    # Reshape the image into a 2D array of pixels
    pixels = image.reshape((-1, 3))
    # Convert to float for better precision in k-means
    pixels = np.float32(pixels)

    # Define criteria and apply k-means clustering
    criteria = (cv2.TERM_CRITERIA_EPS + cv2.TERM_CRITERIA_MAX_ITER, 100, 0.2)
    _, labels, centers = cv2.kmeans(pixels, k, None, criteria, 10, cv2.KMEANS_RANDOM_CENTERS)
    # Take the first center as the dominant color
    dominant_color = np.uint8(centers[0])

    return dominant_color

# Color naming
color_names = {
    "vanilla": (255, 248, 220),  # Creamy off-white, typical of vanilla ice cream
    "chocolate": (90, 58, 34),  # Rich brown, representative of chocolate flavors
    "strawberry": (235, 76, 66),  # Pinkish-red, common for strawberry ice cream
    "mint": (62, 180, 137),  # Mint green, indicative of mint-flavored ice cream
    "blueberry": (70, 65, 150),  # Deep blue or purple, for blueberry flavors
    "pistachio": (147, 197, 114),  # Light green, typical of pistachio ice cream
    "lemon": (255, 247, 0),  # Bright yellow, for lemon or citrus flavors
    "coffee": (111, 78, 55),  # Dark brown, representing coffee or mocha flavors
    "caramel": (255, 188, 117),  # Caramel or butterscotch, a light brown or tan
    "peach": (255, 229, 180),  # Peach or apricot flavors, a soft orange or peach color
    "raspberry": (227, 11, 92),  # Bright pink or fuchsia, common for raspberry flavors
    "blackberry": (34, 34, 59),  # Dark purple or almost black, for blackberry ice cream
    "mango": (252, 184, 19),  # Vibrant orange, typical of mango flavors
    "green tea": (181, 211, 156),  # Pale green, associated with green tea or matcha flavors
    "cookies and cream": (217, 216, 205),  # A mix of white and black, representing the cookie bits
    "red velvet": (130, 0, 0),  # Dark red or maroon, for red velvet cake flavors
    "cotton candy": (250, 114, 255),  # Bright pink or light purple, for cotton candy ice cream
    "bubblegum": (255, 113, 206),  # Pink, similar to traditional bubblegum
    "lavender": (201, 160, 220),  # Light purple, for lavender or floral flavors
    "red": (255, 0, 0),
    "green": (0, 128, 0),
    "blue": (0, 0, 255),
    "yellow": (255, 255, 0),
    "orange": (255, 165, 0),
    "purple": (128, 0, 128),
    "brown": (165, 42, 42),
    "black": (0, 0, 0),
    "gray": (128, 128, 128),
    "white": (255, 255, 255)
}

def closest_color(rgb_value):
    min_colors = {}
    for name, key in color_names.items():
        r_c, g_c, b_c = key
        rd = (r_c - rgb_value[0]) ** 2
        gd = (g_c - rgb_value[1]) ** 2
        bd = (b_c - rgb_value[2]) ** 2
        min_colors[(rd + gd + bd)] = name
    return min_colors[min(min_colors.keys())]

def detect_objects_and_color(frame):
    # Convert the frame to RGB color space
    frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    # Run detection
    predictions = run_detection(frame_rgb)
    # Decode the predictions
    decoded_predictions = decode_predictions(predictions.numpy())[0]

    # Check for 'person' and 'ice_cream' in the predictions
    person_detected = any(pred[1] == 'person' for pred in decoded_predictions)
    ice_cream_detected = any(pred[1] == 'ice_cream' for pred in decoded_predictions)

    # Initialize color name as None
    color_name = None
    # Find the dominant color if ice cream is detected
    if ice_cream_detected:
        dominant_color = find_dominant_color(frame)
        color_name = closest_color(dominant_color)

    return person_detected, ice_cream_detected, color_name

# COMMAND ----------

video_path1 = "/dbfs/FileStore/tables/Ice_Cream_Without_A_Machine_2-2.mp4"  # Adjust the path to your video
cap = cv2.VideoCapture(video_path1)

# Get the total number of frames in the video
total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
# Calculate 5% of the total frames to analyze
frames_to_process = int(total_frames * 0.05)
# Calculate the step size to evenly select frames
step_size = total_frames // frames_to_process

print(f"Total Frames: {total_frames}, Frames to Analyze: {frames_to_process}")

# COMMAND ----------

dbutils.fs.head('dbfs:/FileStore/tables/Ice_Cream_Without_A_Machine_2-2.mp4')
#dbfs:/FileStore/tables/Ice_Cream_Without_A_Machine_2-2.mp4'

# COMMAND ----------

# Initialize a frame counter
frame_counter = 0
# List to hold detection results
detection_results = []

while True:
    success, frame = cap.read()
    if not success:
        break  # Exit the loop if no frame is returned

    if frame_counter % step_size == 0:
        # Detect objects and color in the frame
        person_detected, ice_cream_detected, color_name = detect_objects_and_color(frame)
        # Append the results
        detection_results.append((frame_counter, person_detected, ice_cream_detected, color_name))
        # Print the results for the current frame
        print(f"Frame {frame_counter}: Person: {'Detected' if person_detected else 'Not Detected'}, Ice Cream: {'Detected' if ice_cream_detected else 'Not Detected'}, Color: {color_name if color_name is not None else 'N/A'}")

    frame_counter += 1  # Increment the frame counter

# Release the video capture object
cap.release()

# Print the total number of processed frames and the detection results
print(f"Total frames processed: {len(detection_results)}")