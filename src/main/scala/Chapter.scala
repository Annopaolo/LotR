case class Book(Case : String) {
  override def toString: String = Case
}

case class Chapter(
                  BookName : Book,
                  ChapterName : String,
                  ChapterData: String
                  )
