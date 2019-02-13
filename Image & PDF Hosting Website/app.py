import os
import datetime
import time
import cv2
import numpy
from flask import Flask, flash, request, redirect, url_for, render_template, send_from_directory
from werkzeug.utils import secure_filename
from PIL import Image
from threading import Timer
from threading import Thread
from pdfrw import PdfReader



UPLOAD_FOLDER = 'uploads/'

ALLOWED_EXTENSIONS  = set(['png', 'jpg', 'jpeg', 'pdf'])
ALLOWED_IMGS  = set(['png', 'jpg', 'jpeg'])


app = Flask(__name__)
app.debug=True
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024

def modification_date(filename):
    t = os.path.getmtime(filename)
    return datetime.datetime.fromtimestamp(t)


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def is_img(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_IMGS

def auto_delete(delay, path):
    time.sleep(delay)
    if path:
        os.remove(path)
    return

def count_words(data):
   words = data.split(" ")
   num_words = len(words)
   return num_words
    

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
			
        file = request.files['file']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
			
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            path = os.path.dirname(os.path.abspath(__file__))
            creationDate = os.path.getctime(os.path.dirname(os.path.abspath(__file__)))
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

            del_thread = Thread(target=auto_delete, args=(300, os.path.join(app.config['UPLOAD_FOLDER'], filename)))
            del_thread.start()

            return redirect(url_for('uploaded_file', filename=filename, creationDate=creationDate ))
    else:
        return render_template("index.html")

		
@app.route('/<filename>/<creationDate>/')
def uploaded_file(filename,creationDate):
    path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    uploadDate = modification_date(path)
    fileSize = os.path.getsize(path)
    if( is_img(filename) ):
        im = Image.open(path)
        widthPixels, heightPixels = im.size
        cv2Img = cv2.imread(path)
        avgColorPerRow = numpy.average(cv2Img, axis=0)
        avgColor = numpy.average(avgColorPerRow, axis=0)
        avgRed = round(( float(avgColor[0]) ),2)
        avgGreen = round(( float(avgColor[1]) ),2)
        avgBlue = round(( float(avgColor[2]) ),2)
        return render_template("uploaded.html", filename = filename, upload = uploadDate, creation = creationDate, size = fileSize/1000, width = widthPixels, height = heightPixels, avgRed = avgRed, avgGreen = avgGreen, avgBlue = avgBlue  )
    else:
        pdf = PdfReader(path)
        pageSize = pdf.pages[0].MediaBox
        pageWidth = float(pageSize[2]) / 72 # Convert to inches
        pageHeight = float(pageSize[3]) / 72 
        numLines = sum(1 for line in open(path))

        f = open(path, "r")
        data = f.read()
        f.close()
        numWords = count_words(data)

        return render_template("uploaded.html", filename = filename, upload = uploadDate, creation = creationDate, size = fileSize/1000, pdf=filename, pageWidth = pageWidth, pageHeight = pageHeight, numLines = numLines, numWords = numWords  )

@app.route('/uploads/<filename>', methods=['GET', 'POST'])
def view_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)


@app.route('/delete/<filename>', methods=['GET', 'POST'])
def delete_file(filename):
    os.remove(os.path.join(app.config['UPLOAD_FOLDER'], filename))
    return redirect(url_for('upload_file'))
    
    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)