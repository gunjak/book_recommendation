from flask import Flask,request,render_template
import  function
app=Flask(__name__)

@app.route('/',methods=['Get','POST'])
def homepage():
    return  render_template("index.html")

@app.route("/recommend",methods=['POST','GET'])
def recommen():
    if request.method=='POST':
        obj=function.recommendation()
        books_list=obj.recommend(request.form["name"])
        return render_template('result.html',name=books_list)
if __name__=="__main__":
    app.run(debug=True)




