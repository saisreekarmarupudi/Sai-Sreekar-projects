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
# Model downloading logic remains the same...

import tensorflow as tf
print("Loading EfficientNetB0 model...")
model = tf.keras.applications.EfficientNetB0(weights='imagenet')
print("Model loaded.")


# COMMAND ----------

from tensorflow.keras.applications.efficientnet import preprocess_input, decode_predictions

def run_detection(image):
    resized_image = tf.image.resize(image, (224, 224))
    preprocessed_image = preprocess_input(resized_image)
    input_tensor = tf.convert_to_tensor([preprocessed_image], dtype=tf.float32)
    predictions = model(input_tensor)
    return predictions

def detect_person_eating_icecream(frame):
    frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    predictions = run_detection(frame_rgb)
    decoded_predictions = decode_predictions(predictions.numpy())[0]
    
    # Check if 'person' or 'ice cream' is in the top 5 predictions
    person_detected = any(pred[1] == 'person' for pred in decoded_predictions)
    ice_cream_detected = any(pred[1] == 'ice_cream' for pred in decoded_predictions)
    
    return person_detected, ice_cream_detected


# COMMAND ----------

video_path1 = "/dbfs/FileStore/tables/Ice_Cream_Without_A_Machine_2-2.mp4"  # Adjust the path to your video
cap = cv2.VideoCapture(video_path1)

# Get the total number of frames in the video
total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

# Calculate 5% of the total frames
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
        break  # Break the loop if there are no frames left to process
    
    # Process only if the frame_counter is divisible by the step_size
    if frame_counter % step_size == 0:
        person_detected, ice_cream_detected = detect_person_eating_icecream(frame)
        detection_results.append((frame_counter, person_detected, ice_cream_detected))
        print(f"Frame {frame_counter}: Person detected: {person_detected}, Ice cream detected: {ice_cream_detected}")
    
    frame_counter += 1

# Release the video capture object
cap.release()

# Print total processed frames and detection results
print(f"Total frames processed: {len(detection_results)}")
