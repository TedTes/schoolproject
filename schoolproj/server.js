const express=require('express');
const csv=require('csv-parser');
const fs=require('fs');

const app=express();

//counter to check all files are read
let count=0;
let marks=[],students=[],courses=[],tests=[];

const files=process.argv.slice(2);

files.map(file=>{
  readFile(file);
});

function readFile(file){
  if(file!==undefined){
    fs.createReadStream(file)
    .pipe(csv())
    .on('data',(row)=>{
        if(Object.keys(row).length!==0){
            switch(file){
                case 'marks.csv':
                marks.push(row);
               break;
                case 'students.csv':
                students.push(row);
                  break;
                case 'tests.csv':
                   tests.push(row);
                 break;
               case 'courses.csv':
                  courses.push(row);
                   break;
               }
        }
      }).on('end',()=>{
        count++;
      if(count===files.length ){
        const checkRes=checkTestsFile(tests);
        if(checkRes!==undefined){
          console.log(JSON.stringify({
            "error":"invalid course weights"
          }))
                   
        }
     else{
      let courseResult=0;
      //array to map course average to course
       const courseAvg=[];
       const  coursesData=[];
       //Ids to map course with result
       const courseIds=[];
     
      students.map(student=>{
        const studentMarks=marks.filter(mark=>student.id===mark.student_id);
     
              const studentTests=tests.filter(test=>studentMarks.map(sMark=>{

                 if( sMark.test_id===test.id){
                         const mark=sMark.mark;
                        const weight=test.weight;
                       
                        courseResult=(mark/100)*weight
                        const length=studentMarks.length;
                        const courseId=test.course_id;

                         courseIds.push(courseId)
                        coursesData.push({mark,weight,courseId,courseResult});

                       const res=getCourseAverage(coursesData,courseIds,length);
                
                   if(res!==undefined){
                         for(let i=0;i<res.length;i++){
                        let sumCourseAverage=0;
                        let id=0
                           const result=res[i].map((dta)=>{
                              sumCourseAverage+=dta.courseResult;
                               id=dta.courseId
                            });
                           courseAvg.push({id,sumCourseAverage})
                         }
                     
                         return  sMark.test_id===test.id;
                   
                       }
                    }
             }))
            const studentCourses=courses.filter(course=>studentTests.map(sTest=>sTest.course_id===course.id))
            student['totalAverage']=0;
             for(let i=0;i<studentCourses.length;i++){
              studentCourses[i]['courseAverage']=courseAvg[i].sumCourseAverage;
          
              student['totalAverage']=(student['totalAverage']+(courseAvg[i].sumCourseAverage/courseAvg.length))
             }
             student['totalAverage']=student['totalAverage'].toFixed(2);
           const sortedStudents=[{...student,"courses":studentCourses}].sort((a,b)=>{
         return a.id-b.id;
           })
      const res={
            students:sortedStudents
          }
          console.log(JSON.stringify(res,null,1));
          })
 }
    
      }
   
           
})
   }
   else{
     console.log("invalid files")
   }
}
function  getCourseAverage(coursesData,courseIds,length){

if(coursesData.length===length && courseIds.length===length){
    const uniqueCourseIds=[...new Set(courseIds)]
    const res=uniqueCourseIds.map(id=>{
      return coursesData.filter(dta=>(id===dta.courseId))
    })
    return res ;
}
}
function checkTestsFile(tests){
const sortedTests=tests.slice().sort((a,b)=>a.id-b.id)
let prev=0;
const res=sortedTests.reduce((arr,dta)=>{
if(prev===dta.course_id){
 arr[arr.length-1]+=parseInt(dta.weight)
}
else{
  prev=dta.course_id
  arr.push(parseInt(dta.weight));
}
return arr;
},[]);
return res.find(no=>no!==100)
}

const port=3000;
app.listen(port,()=>{
    console.log(`server started running on port ${port}`)
});