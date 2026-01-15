import { useState, useEffect } from 'react'

function NewsCard({ item }) {
  const categoryColors = {
    music: 'bg-pink-500',
    social: 'bg-purple-500',
    appearance: 'bg-blue-500',
    fan: 'bg-green-500',
    industry: 'bg-orange-500',
  }

  return (
    <div className="bg-white/10 backdrop-blur-sm rounded-lg p-6 hover:bg-white/15 transition-all">
      <div className="flex items-start gap-4">
        <div className="flex-1">
          <div className="flex items-center gap-2 mb-2">
            <span className={`${categoryColors[item.category] || 'bg-gray-500'} text-white text-xs px-2 py-1 rounded-full uppercase font-medium`}>
              {item.category}
            </span>
            {item.relevance_score && (
              <span className="text-yellow-400 text-sm">
                {'★'.repeat(Math.min(item.relevance_score, 5))}
              </span>
            )}
          </div>
          <h3 className="text-white text-xl font-bold mb-2">{item.headline}</h3>
          <p className="text-gray-300 mb-3">{item.summary}</p>
          <div className="flex items-center gap-4 text-sm text-gray-400">
            {item.source_name && <span>{item.source_name}</span>}
            {item.published_date && (
              <span>{new Date(item.published_date).toLocaleDateString()}</span>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

function TrendingTopics({ topics }) {
  if (!topics || topics.length === 0) return null

  return (
    <div className="mb-8">
      <h2 className="text-white text-lg font-semibold mb-3">Trending</h2>
      <div className="flex flex-wrap gap-2">
        {topics.map((topic, index) => (
          <span
            key={index}
            className="bg-gradient-to-r from-pink-500 to-purple-500 text-white px-4 py-2 rounded-full text-sm font-medium"
          >
            {topic}
          </span>
        ))}
      </div>
    </div>
  )
}

function App() {
  const [news, setNews] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    fetch('/api/news')
      .then(res => {
        if (!res.ok) throw new Error('Failed to fetch news')
        return res.json()
      })
      .then(data => {
        setNews(data)
        setLoading(false)
      })
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }, [])

  return (
    <div className="min-h-screen">
      {/* Header */}
      <header className="border-b border-white/10 py-6">
        <div className="max-w-4xl mx-auto px-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold bg-gradient-to-r from-pink-500 to-purple-500 text-transparent bg-clip-text">
                KATSEYE News
              </h1>
              <p className="text-gray-400 text-sm mt-1">
                Latest updates from your favorite K-pop group
              </p>
            </div>
            <div className="text-right text-sm text-gray-500">
              {news?.last_updated && (
                <span>
                  Updated: {new Date(news.last_updated).toLocaleString()}
                </span>
              )}
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-4xl mx-auto px-4 py-8">
        {loading && (
          <div className="text-center py-20">
            <div className="inline-block animate-spin rounded-full h-12 w-12 border-4 border-pink-500 border-t-transparent"></div>
            <p className="text-gray-400 mt-4">Loading news...</p>
          </div>
        )}

        {error && (
          <div className="bg-red-500/10 border border-red-500/50 rounded-lg p-6 text-center">
            <p className="text-red-400">{error}</p>
          </div>
        )}

        {news && !loading && (
          <>
            <TrendingTopics topics={news.trending_topics} />

            <div className="space-y-4">
              {news.news_items?.map(item => (
                <NewsCard key={item.id} item={item} />
              ))}
            </div>

            {news.news_items?.length === 0 && (
              <div className="text-center py-20 text-gray-400">
                <p>No news available yet. Check back soon!</p>
              </div>
            )}
          </>
        )}
      </main>

      {/* Footer */}
      <footer className="border-t border-white/10 py-6 mt-8">
        <div className="max-w-4xl mx-auto px-4 text-center text-gray-500 text-sm">
          <p>Powered by Grep Research | Data updated daily</p>
        </div>
      </footer>
    </div>
  )
}

export default App
